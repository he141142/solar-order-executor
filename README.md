# solar-order-executor

ECS Fargate worker. It reads `TradeIntent`s from SQS, places orders on the exchange (mTLS), saves results to DynamoDB, and publishes `OrderFilled` / `OrderRejected`. It serves every zone; the zone comes from each intent.

**This repo only holds the code.** The ECS cluster/service, the task definition (which points at the `:<env>` tag), SQS, autoscaling and alarms live in [`solar-infra`](https://github.com/he141142/solar-infra), in the `trading` project's `app` layer.

## Release
| Trigger | Result |
|---|---|
| PR | vet + test + build |
| Push `main` | build → push `:<sha>` and move `:dev` → `update-service --force-new-deployment` on dev |
| Run workflow manually, pick `stg` / `prd` | same steps against that environment's account (GitHub environment requires approval) |

The service has `minHealthyPercent=100` and a circuit breaker: a new task that fails to start rolls back automatically. **Manual rollback:** re-run the workflow on the old commit.

## One-time GitHub setup
- Environments `dev`, `stg`, `prd`, each with secret `AWS_DEPLOY_ROLE_ARN` = output `OrderExecutorDeployRoleArn` of the `trading-<env>-foundation` stack.
- If `solar-shared` is private: secret `GO_MODULES_TOKEN`.
- `stg`/`prd`: enable required reviewers.

## Local development
```bash
go vet ./... && go test ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/executor .
docker build --platform linux/amd64 -t order-executor .
```

Environment variables (set by infra): `QUEUE_URL`, `TABLE_NAME`, `EVENT_BUS_NAME`, `EXCHANGE_MTLS` (from Secrets Manager). Config: Parameter Store `/solar-trading/exchange/*` (`mode` = `mock` | `live`).
