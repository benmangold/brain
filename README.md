# brain

a brain for my macbook

## folds

each folder is a `fold` of the brain, a dedicated process or layer of the stack

### persistence (memory)

a multipurpose persistence layer

docker-managed databases composed together, data managed with mounted volumes to allow easy management and backups

includes:

- postgresql - relational database
- pg-vector - dedicated vector store
- redis - key-value store
- mongodb
- dynamodb

### orchestration (sub-conscious thought)

data orchestration with dagster

- runs arbitrary python code
- prompts local `ollama` LLMs
- writes to persistence layer  
- calls external APIs
