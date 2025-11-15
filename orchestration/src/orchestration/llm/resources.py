"""
Dagster resources for Ollama LLM integration.
Encapsulates all Ollama interactions and configurations.
"""

from dagster import ConfigurableResource, InitResourceContext
from typing import List, Dict, Any, Optional
from pydantic import Field
import ollama
from datetime import datetime


class OllamaResource(ConfigurableResource):
    """Resource for interacting with Ollama LLM models."""

    primary_model: str = Field(
        default="llama3.2:1b",
        description="Primary Ollama model to use"
    )
    fallback_model: Optional[str] = Field(
        default=None,
        description="Fallback model if primary fails"
    )
    temperature: float = Field(
        default=0.7,
        description="Sampling temperature (0.0-1.0)"
    )
    max_tokens: int = Field(
        default=500,
        description="Maximum tokens to generate"
    )
    timeout: int = Field(
        default=30,
        description="Request timeout in seconds"
    )
    num_ctx: int = Field(
        default=2048,
        description="Context window size"
    )

    def _log(self, message: str, level: str = "info"):
        """Simple logging helper."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [{level.upper()}] {message}")

    def pull_model(self, model_name: str) -> bool:
        """
        Pull/download an Ollama model if not already available.

        Args:
            model_name: Name of the model to pull

        Returns:
            True if successful, False otherwise
        """
        try:
            self._log(f"Pulling model: {model_name}")
            ollama.pull(model_name)
            self._log(f"Successfully pulled model: {model_name}")
            return True
        except Exception as e:
            self._log(f"Error pulling model {model_name}: {e}", "error")
            return False

    def list_available_models(self) -> List[str]:
        """
        List all locally available Ollama models.

        Returns:
            List of model names
        """
        try:
            models = ollama.list()
            return [m['name'] for m in models.get('models', [])]
        except Exception as e:
            self._log(f"Error listing models: {e}", "error")
            return []

    def test_model(self, model_name: str) -> Dict[str, Any]:
        """
        Test if a model is working with a simple prompt.

        Args:
            model_name: Name of the model to test

        Returns:
            Dict with status and test response
        """
        try:
            response = ollama.chat(
                model=model_name,
                messages=[{'role': 'user', 'content': 'Say "OK"'}],
                options={'num_ctx': 128}
            )

            return {
                'status': 'ready',
                'model': model_name,
                'test_response': response['message']['content']
            }
        except Exception as e:
            return {
                'status': 'failed',
                'model': model_name,
                'error': str(e)
            }

    def ensure_models_available(self, models: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Ensure specified models are available, pulling if necessary.

        Args:
            models: List of model names to check. If None, uses primary and fallback models.

        Returns:
            Dict mapping model names to their status info
        """
        if models is None:
            models = [self.primary_model]
            if self.fallback_model:
                models.append(self.fallback_model)

        results = {}

        for model_name in models:
            # Pull the model
            pull_success = self.pull_model(model_name)

            if pull_success:
                # Test the model
                test_result = self.test_model(model_name)
                results[model_name] = test_result
            else:
                results[model_name] = {
                    'status': 'failed',
                    'model': model_name,
                    'error': 'Failed to pull model'
                }

        return results

    def get_active_model(self, available_models: Dict[str, Dict[str, Any]]) -> str:
        """
        Determine which model to use based on availability.

        Args:
            available_models: Dict of model statuses from ensure_models_available

        Returns:
            Name of the model to use

        Raises:
            Exception if no models are available
        """
        # Try primary model first
        if available_models.get(self.primary_model, {}).get('status') == 'ready':
            return self.primary_model

        # Try fallback model
        if self.fallback_model and available_models.get(self.fallback_model, {}).get('status') == 'ready':
            self._log(f"Primary model unavailable, using fallback: {self.fallback_model}", "warning")
            return self.fallback_model

        raise Exception("No available models for processing")

    def chat(
            self,
            prompt: str,
            model: Optional[str] = None,
            system_prompt: Optional[str] = None,
            temperature: Optional[float] = None,
            max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Send a chat completion request to Ollama.

        Args:
            prompt: User prompt to send
            model: Model to use (defaults to primary_model)
            system_prompt: Optional system prompt
            temperature: Override default temperature
            max_tokens: Override default max_tokens

        Returns:
            Dict with response and token counts
        """
        model = model or self.primary_model
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens

        messages = []

        if system_prompt:
            messages.append({
                'role': 'system',
                'content': system_prompt
            })

        messages.append({
            'role': 'user',
            'content': prompt
        })

        try:
            response = ollama.chat(
                model=model,
                messages=messages,
                options={
                    'temperature': temperature,
                    'num_predict': max_tokens,
                    'num_ctx': self.num_ctx,
                }
            )

            return {
                'success': True,
                'response': response['message']['content'],
                'model': model,
                'tokens_input': response.get('prompt_eval_count', 0),
                'tokens_output': response.get('eval_count', 0),
                'total_tokens': response.get('prompt_eval_count', 0) + response.get('eval_count', 0),
            }

        except Exception as e:
            self._log(f"Error during chat completion: {e}", "error")
            return {
                'success': False,
                'error': str(e),
                'model': model,
            }

    def batch_chat(
            self,
            prompts: List[Dict[str, str]],
            model: Optional[str] = None,
            system_prompt: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Process multiple prompts in batch.

        Args:
            prompts: List of dicts with 'id' and 'prompt' keys
            model: Model to use (defaults to primary_model)
            system_prompt: Optional system prompt for all requests

        Returns:
            List of response dicts with results
        """
        results = []

        for prompt_data in prompts:
            prompt_id = prompt_data.get('id', 'unknown')
            prompt_text = prompt_data.get('prompt', '')

            self._log(f"Processing prompt: {prompt_id}")

            result = self.chat(
                prompt=prompt_text,
                model=model,
                system_prompt=system_prompt
            )

            # Add the prompt ID to the result
            result['id'] = prompt_id
            result['prompt'] = prompt_text

            # Add any additional metadata from prompt_data
            for key, value in prompt_data.items():
                if key not in ['id', 'prompt']:
                    result[key] = value

            results.append(result)

        return results


class DataProcessingResource(ConfigurableResource):
    """Resource for data processing utilities."""

    batch_size: int = Field(
        default=5,
        description="Number of records to process per batch"
    )
    save_outputs: bool = Field(
        default=True,
        description="Whether to save outputs to disk"
    )
    output_dir: str = Field(
        default="tmp/dagster_ollama_outputs",
        description="Directory for saving outputs"
    )

    def create_prompt(self, text: str, task: str, expected_length: str = "concise") -> str:
        """
        Create a prompt based on task type.

        Args:
            text: Input text to process
            task: Type of task (summarize, sentiment, extract, classify, keywords)
            expected_length: Expected output format/length

        Returns:
            Formatted prompt string
        """
        task_templates = {
            'summarize': f"Summarize this text in {expected_length}: {text}",
            'sentiment': f"What is the sentiment ({expected_length})? Text: {text}",
            'extract': f"Extract items as a {expected_length}: {text}",
            'classify': f"Classify this into a {expected_length}: {text}",
            'keywords': f"Extract {expected_length}: {text}",
            'general': text
        }

        return task_templates.get(task, text)

    def get_output_path(self, filename: str) -> str:
        """
        Get full path for output file.

        Args:
            filename: Name of the output file

        Returns:
            Full path string
        """
        from pathlib import Path

        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        return str(output_path / filename)

    def create_batches(self, data: List[Any]) -> List[List[Any]]:
        """
        Split data into batches.

        Args:
            data: List of items to batch

        Returns:
            List of batches
        """
        batches = []
        for i in range(0, len(data), self.batch_size):
            batches.append(data[i:i + self.batch_size])
        return batches