from langchain_ollama import ChatOllama
from pathlib import Path
import time


class StoryWriterAgent:
    def __init__(self, file_path: str = "fantasy_story.agent.md"):
        self.model = ChatOllama(
            model="gpt-oss:20b",
            validate_model_on_init=True,
            temperature=0.9,  # Higher for more creative writing
            num_predict=512,  # More tokens for story content
        )
        self.file_path = Path(file_path)

        # Initialize empty story file
        if not self.file_path.exists():
            self.file_path.write_text("# Fantasy Story\n\n", encoding="utf-8")

    def read_story(self) -> str:
        """Read the current story content."""
        return self.file_path.read_text(encoding="utf-8")

    def append_to_story(self, content: str):
        """Append new content to the story."""
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(content)

    def write_next_part(self, instruction: str = "Continue the story"):
        """Generate and append the next part of the story."""
        current_story = self.read_story()

        system_prompt = (
            "You are a creative fantasy story writer. Write engaging, descriptive fantasy fiction. "
            "Continue the story naturally from where it left off. "
            "Write 2-3 paragraphs at a time. Use vivid descriptions and interesting characters."
        )

        messages = [
            ("system", system_prompt),
            ("human", f"Current story so far:\n\n{current_story}\n\n{instruction}"),
        ]

        print(f"🖊️  Generating next part... ({instruction})")
        response = self.model.invoke(messages)
        new_content = response.content.strip()

        # Add spacing and append
        formatted_content = f"\n\n{new_content}"
        self.append_to_story(formatted_content)

        print(f"✅ Added {len(new_content)} characters to story")
        return new_content

    def start_story(self, premise: str):
        """Start a new story with a given premise."""
        system_prompt = (
            "You are a creative fantasy story writer. Write the opening of a fantasy story. "
            "Write 2-3 paragraphs that introduce the setting and main character. "
            "Make it engaging and atmospheric."
        )

        messages = [
            ("system", system_prompt),
            (
                "human",
                f"Write the beginning of a fantasy story with this premise: {premise}",
            ),
        ]

        print(f"🖊️  Starting new story...")
        response = self.model.invoke(messages)
        opening = response.content.strip()

        # Write fresh story
        self.file_path.write_text(f"# Fantasy Story\n\n{opening}", encoding="utf-8")

        print(f"✅ Story started with {len(opening)} characters")
        return opening

    def develop_story(self, num_parts: int = 5, delay: float = 1.0):
        """Progressively write multiple parts of the story."""
        for i in range(num_parts):
            print(f"\n--- Part {i + 1}/{num_parts} ---")
            self.write_next_part()

            if i < num_parts - 1:  # Don't delay after last part
                time.sleep(delay)

        print(f"\n✨ Story complete! Saved to: {self.file_path}")

    def add_plot_twist(self):
        """Add an unexpected plot twist."""
        return self.write_next_part(
            "Continue the story, but introduce an unexpected plot twist that changes everything"
        )

    def add_character(self, character_description: str):
        """Introduce a new character."""
        return self.write_next_part(
            f"Continue the story by introducing a new character: {character_description}"
        )

    def end_story(self):
        """Conclude the story."""
        return self.write_next_part(
            f"Conclude the story.  Make sure to end with enough tokens to ensure a complete sentence."
        )


if __name__ == "__main__":
    agent = StoryWriterAgent("my_fantasy_tale.agent.md")

    # Start a new story
    agent.start_story(
        "A young mage discovers they can see through the eyes of dragons, "
        "but each vision brings them closer to becoming one themselves"
    )

    # Write the story progressively
    print("\n" + "=" * 50)
    print("📖 Developing the story...")
    print("=" * 50)

    agent.develop_story(num_parts=3, delay=2.0)

    # Add a plot twist
    print("\n" + "=" * 50)
    print("🌪️  Adding plot twist...")
    print("=" * 50)
    agent.add_plot_twist()

    # Add a new character
    print("\n" + "=" * 50)
    print("👤 Introducing new character...")
    print("=" * 50)
    agent.add_character("a mysterious wanderer with knowledge of ancient dragon lore")

    # Continue a bit more
    agent.develop_story(num_parts=2, delay=2.0)

    agent.end_story()

    # Show final preview
    print("\n" + "=" * 50)
    print("📜 STORY PREVIEW (first 500 chars):")
    print("=" * 50)
    story = agent.read_story()
    print(story[:500] + "...\n")
