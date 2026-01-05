from langchain_ollama import ChatOllama
from pathlib import Path


class FileManagerAgent:
    def __init__(self, file_path: str = "managed_file.md"):
        self.model = ChatOllama(
            model="gpt-oss:20b",
            validate_model_on_init=True,
            temperature=0.8,
            num_predict=256,
        )
        self.file_path = Path(file_path)

        # Ensure the file exists
        if not self.file_path.exists():
            self.file_path.touch()

    def read_file(self) -> str:
        """Read the contents of the markdown file."""
        try:
            return self.file_path.read_text(encoding="utf-8")
        except Exception as e:
            return f"Error reading file: {str(e)}"

    def write_file(self, content: str) -> str:
        """Write content to the markdown file, replacing existing content."""
        try:
            self.file_path.write_text(content, encoding="utf-8")
            return "File written successfully."
        except Exception as e:
            return f"Error writing file: {str(e)}"

    def append_file(self, content: str) -> str:
        """Append content to the end of the markdown file."""
        try:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(content)
            return "Content appended successfully."
        except Exception as e:
            return f"Error appending to file: {str(e)}"

    def invoke(self, user_message: str):
        """Send a message to the agent about the file."""
        file_content = self.read_file()

        system_prompt = (
            f"You are a helpful file management assistant. "
            f"You're managing a markdown file at: {self.file_path}\n"
            f"Current file content:\n```\n{file_content}\n```"
        )

        messages = [("system", system_prompt), ("human", user_message)]

        response = self.model.invoke(messages)
        return response.content


if __name__ == "__main__":
    # Initialize with a specific file
    agent = FileManagerAgent("notes.md")

    # # Write some initial content
    # agent.write_file("# My Notes\n\nThis is a test file.\n")

    # Ask the agent about the file
    result = agent.invoke("What's in the file? Summarize it.")
    print("Agent response:", result)

    # Append some content
    agent.append_file("\n## New Section\n- Item 1\n")

    # Ask again
    result = agent.invoke("What sections are in the file now?")
    print("Agent response:", result)
