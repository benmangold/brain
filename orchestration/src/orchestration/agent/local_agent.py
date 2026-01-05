from langchain_ollama import ChatOllama


class LocalAgent:
    def __init__(self):
        self.model = ChatOllama(
            model="gpt-oss:20b",
            validate_model_on_init=True,
            temperature=0.8,
            num_predict=256,
            # other params ...
        )
        self.system_prompts = [
            (
                "system",
                "You are a helpful translator. Translate the user sentence to French.",
            ),
        ]

    def invoke(self, messages: list[tuple[str, str]]):
        if not messages:
            raise Exception
        return self.model.invoke(self.system_prompts + messages)


if __name__ == "__main__":
    agent = LocalAgent()
    result = agent.invoke([("human", "I love programming.")])
    print(result)
