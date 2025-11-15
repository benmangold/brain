"""
Dagster assets for orchestrating Ollama LLM calls locally.
Install: pip install dagster ollama
Run: dagster dev
"""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Definitions,
    Config,
)
from typing import List, Dict, Any
import ollama
import json


class OllamaConfig(Config):
    """Configuration for Ollama model interactions."""
    model: str = "llama3.2:1b"
    temperature: float = 0.7
    num_ctx: int = 2048


@asset(
    description="Pull the Ollama model if not already available locally"
)
def ollama_model(context: AssetExecutionContext, config: OllamaConfig) -> str:
    """Ensure the specified Ollama model is available locally."""
    try:
        # Pull the model (idempotent - won't re-download if exists)
        context.log.info(f"Ensuring model {config.model} is available...")
        ollama.pull(config.model)

        # Verify model is available
        models = ollama.list()
        model_names = [m['model'] for m in models['models']]

        if config.model in model_names or f"{config.model}:latest" in model_names:
            context.log.info(f"Model {config.model} is ready")
            return config.model
        else:
            raise Exception(f"Model {config.model} not found after pull")

    except Exception as e:
        context.log.error(f"Error with model setup: {e}")
        raise


@asset(
    description="Generate sample prompts for LLM processing",
    deps=[ollama_model]
)
def sample_prompts(context: AssetExecutionContext) -> List[Dict[str, str]]:
    """Create a list of sample prompts to process."""
    prompts = [
        {
            "id": "sentiment_1",
            "prompt": "Analyze the sentiment of this text: 'I love using Dagster for data orchestration!'",
            "task": "sentiment_analysis"
        },
        {
            "id": "summarize_1",
            "prompt": "Summarize in one sentence: Data orchestration helps teams coordinate complex data workflows efficiently.",
            "task": "summarization"
        },
        {
            "id": "extract_1",
            "prompt": "Extract key technologies from: 'Our stack uses Python, Dagster, Ollama, and local LLMs for processing.'",
            "task": "extraction"
        }
    ]

    context.log.info(f"Generated {len(prompts)} sample prompts")
    return prompts


@asset(
    description="Process prompts through Ollama LLM and collect responses"
)
def llm_responses(
        context: AssetExecutionContext,
        config: OllamaConfig,
        ollama_model: str,
        sample_prompts: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Send prompts to Ollama and collect responses."""
    responses = []

    for prompt_data in sample_prompts:
        context.log.info(f"Processing prompt: {prompt_data['id']}")

        try:
            # Make the LLM call
            response = ollama.chat(
                model=config.model,
                messages=[
                    {
                        'role': 'user',
                        'content': prompt_data['prompt']
                    }
                ],
                options={
                    'temperature': config.temperature,
                    'num_ctx': config.num_ctx,
                }
            )

            result = {
                'id': prompt_data['id'],
                'task': prompt_data['task'],
                'prompt': prompt_data['prompt'],
                'response': response['message']['content'],
                'model': config.model,
                'tokens_prompt': response.get('prompt_eval_count', 0),
                'tokens_response': response.get('eval_count', 0),
            }

            responses.append(result)
            context.log.info(f"Completed {prompt_data['id']}: {len(result['response'])} chars")

        except Exception as e:
            context.log.error(f"Error processing {prompt_data['id']}: {e}")
            responses.append({
                'id': prompt_data['id'],
                'error': str(e)
            })

    return responses


@asset(
    description="Aggregate and analyze LLM processing results"
)
def processing_summary(
        context: AssetExecutionContext,
        llm_responses: List[Dict[str, Any]]
) -> MaterializeResult:
    """Create a summary of the LLM processing run."""

    total_prompts = len(llm_responses)
    successful = sum(1 for r in llm_responses if 'response' in r)
    failed = total_prompts - successful

    total_prompt_tokens = sum(r.get('tokens_prompt', 0) for r in llm_responses)
    total_response_tokens = sum(r.get('tokens_response', 0) for r in llm_responses)

    summary = {
        'total_prompts': total_prompts,
        'successful': successful,
        'failed': failed,
        'total_tokens': total_prompt_tokens + total_response_tokens,
        'prompt_tokens': total_prompt_tokens,
        'response_tokens': total_response_tokens,
    }

    # Log sample responses
    context.log.info("Sample responses:")
    for response in llm_responses[:2]:  # Show first 2
        if 'response' in response:
            context.log.info(f"\n[{response['id']}] {response['task']}")
            context.log.info(f"Response: {response['response'][:200]}...")

    return MaterializeResult(
        metadata={
            "total_prompts": total_prompts,
            "successful": successful,
            "failed": failed,
            "total_tokens": total_prompt_tokens + total_response_tokens,
            "summary": MetadataValue.json(summary),
            "sample_response": MetadataValue.md(
                f"**Sample Output:**\n\n{llm_responses[0].get('response', 'N/A')[:300]}..."
                if llm_responses and 'response' in llm_responses[0] else "No responses"
            )
        }
    )
