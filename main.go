// order-executor chạy trên ECS Fargate: đọc TradeIntent từ SQS, đặt lệnh lên sàn,
// lưu kết quả vào DynamoDB và phát OrderFilled / OrderRejected.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/he141142/solar-shared/bus"
	"github.com/he141142/solar-shared/domain"
	"github.com/he141142/solar-shared/zonecfg"
)

type executor struct {
	sqs      *sqs.Client
	ddb      *dynamodb.Client
	eb       *eventbridge.Client
	exchange Exchange
	queueURL string
	table    string
	busName  string
}

var errDuplicate = errors.New("intent already handled")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fatal("load aws config", err)
	}
	// Một executor phục vụ mọi zone; zone nằm trong từng TradeIntent.
	vals, err := zonecfg.New(ssm.NewFromConfig(awsCfg), domain.ExchangeParamPath).Get(ctx)
	if err != nil {
		fatal("load exchange config", err)
	}
	ex, err := newExchange(vals[domain.ParamExchangeMode], vals[domain.ParamExchangeEndpoint], os.Getenv("EXCHANGE_MTLS"))
	if err != nil {
		fatal("init exchange client", err)
	}

	e := &executor{
		sqs:      sqs.NewFromConfig(awsCfg),
		ddb:      dynamodb.NewFromConfig(awsCfg),
		eb:       eventbridge.NewFromConfig(awsCfg),
		exchange: ex,
		queueURL: os.Getenv("QUEUE_URL"),
		table:    os.Getenv("TABLE_NAME"),
		busName:  os.Getenv("EVENT_BUS_NAME"),
	}
	slog.Info("order-executor started", "mode", vals[domain.ParamExchangeMode])
	e.run(ctx)
	slog.Info("order-executor stopped")
}

func (e *executor) run(ctx context.Context) {
	for ctx.Err() == nil {
		out, err := e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &e.queueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("receive", "err", err)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		for _, m := range out.Messages {
			// Không dùng ctx gốc: khi SIGTERM vẫn xử lý xong message đang dở.
			msgCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			e.handle(msgCtx, m)
			cancel()
		}
	}
}

func (e *executor) handle(ctx context.Context, m sqstypes.Message) {
	err := e.process(ctx, aws.ToString(m.Body))
	if err != nil && !errors.Is(err, errDuplicate) {
		// Không xoá message -> SQS giao lại sau visibility timeout, quá 3 lần vào DLQ.
		slog.Error("process order", "err", err, "messageId", aws.ToString(m.MessageId))
		return
	}
	if _, err := e.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &e.queueURL, ReceiptHandle: m.ReceiptHandle}); err != nil {
		slog.Error("delete message", "err", err)
	}
}

func (e *executor) process(ctx context.Context, body string) error {
	// EventBridge -> SQS gửi nguyên envelope của event.
	var env struct {
		Detail domain.TradeIntent `json:"detail"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	in := env.Detail
	if in.IntentID == "" || in.QuantityKwh <= 0 || (in.Side != "BUY" && in.Side != "SELL") {
		return fmt.Errorf("invalid intent: %s", body)
	}

	key := map[string]ddbtypes.AttributeValue{"PK": str("ORDER#" + in.IntentID), "SK": str("META")}
	if err := e.claim(ctx, key, in); err != nil {
		return err
	}

	res, err := e.exchange.Place(ctx, in)
	if err != nil {
		_, _ = e.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &e.table, Key: key})
		return fmt.Errorf("place order %s: %w", in.IntentID, err)
	}

	_, err = e.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                &e.table,
		Key:                      key,
		UpdateExpression:         aws.String("SET #st = :st, exchangeOrderId = :oid, filledKwh = :f, price = :p, GSI1PK = :g"),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":st":  str(res.Status),
			":oid": str(res.OrderID),
			":f":   num(res.FilledKwh),
			":p":   num(res.Price),
			":g":   str("ORDERS#" + res.Status),
		},
	})
	if err != nil {
		return fmt.Errorf("save order %s: %w", in.IntentID, err)
	}

	detailType := domain.OrderFilled
	if res.Status != "FILLED" {
		detailType = domain.OrderRejected
	}
	slog.Info("order done", "intent", in.IntentID, "side", in.Side, "status", res.Status, "kwh", res.FilledKwh, "price", res.Price)
	return bus.Publish(ctx, e.eb, e.busName, domain.SourceExchange, detailType, res)
}

func (e *executor) claim(ctx context.Context, key map[string]ddbtypes.AttributeValue, in domain.TradeIntent) error {
	item := map[string]ddbtypes.AttributeValue{
		"customerId":  str(in.CustomerID),
		"zone":        str(in.Zone),
		"side":        str(in.Side),
		"quantityKwh": num(in.QuantityKwh),
		"limitPrice":  num(in.LimitPrice),
		"status":      str("SUBMITTING"),
		"GSI1PK":      str("ORDERS#SUBMITTING"),
		"GSI1SK":      str(in.Interval),
		"createdAt":   str(time.Now().UTC().Format(time.RFC3339)),
	}
	for k, v := range key {
		item[k] = v
	}
	_, err := e.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &e.table,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	var ccf *ddbtypes.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
		slog.Info("duplicate intent skipped", "intent", in.IntentID)
		return errDuplicate
	}
	return err
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

func str(s string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: s} }

func num(f float64) ddbtypes.AttributeValue {
	return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatFloat(f, 'f', -1, 64)}
}
