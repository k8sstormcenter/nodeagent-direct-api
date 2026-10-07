# nodeagent-direct-api

`nodeagent.direct.v1.AlertStream`: the node-local alert stream node-agent serves and dx consumes (entlein/node-agent#30). The `.proto` is the contract; the generated Go is committed so neither side needs a codegen step.

Regenerate (protoc 29.x, protoc-gen-go 1.36.11, protoc-gen-go-grpc 1.5.1):

    protoc -I . --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative nodeagent/direct/v1/alertstream.proto
