"""
Dagster assets for Ollama LLM orchestration.
Uses resources for all Ollama interactions.
"""

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
)
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
from orchestration.llm.resources import OllamaResource, DataProcessingResource

LLM_CONNECTIVITY_TEST_GROUP_NAME = "llm_connectivity"


@asset(
    description="Verify Ollama models are available and ready",
    retry_policy=RetryPolicy(max_retries=2, delay=5),
    group_name=LLM_CONNECTIVITY_TEST_GROUP_NAME,
)
def available_models(
    context: AssetExecutionContext,
    ollama: OllamaResource,
) -> Dict[str, Any]:
    """
    Check which models are available and pull if needed.

    Returns:
        Dict mapping model names to their status
    """
    context.log.info("Checking Ollama model availability...")

    # Ensure primary and fallback models are available
    model_status = ollama.ensure_models_available()

    # Log results
    for model_name, status in model_status.items():
        if status["status"] == "ready":
            context.log.info(f"✓ Model {model_name} is ready")
        else:
            context.log.warning(
                f"✗ Model {model_name} failed: {status.get('error', 'Unknown error')}"
            )

    # List all available models
    all_models = ollama.list_available_models()
    context.log.info(f"Total models available locally: {len(all_models)}")

    return model_status


@asset(
    description="Load or generate input data for LLM processing",
    group_name=LLM_CONNECTIVITY_TEST_GROUP_NAME,
)
def input_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Create or load input dataset for LLM processing.

    In production, this would load from a database, API, or file.

    Returns:
        DataFrame with text data and processing instructions
    """

    # Sample data - replace with your actual data source
    data = [
        {
            "id": 1,
            "text": "Dagster is a modern data orchestration platform for building, deploying, and monitoring data pipelines.",
            "task": "summarize",
            "expected_length": "one sentence",
        },
        {
            "id": 2,
            "text": "The weather is absolutely terrible today with heavy rain and strong winds.",
            "task": "sentiment",
            "expected_length": "one word: positive, negative, or neutral",
        },
        {
            "id": 3,
            "text": "Our technology stack includes Python, JavaScript, SQL, Docker, and Kubernetes.",
            "task": "extract",
            "expected_length": "comma-separated list",
        },
        {
            "id": 4,
            "text": "Machine learning models can be deployed locally using tools like Ollama for privacy and cost savings.",
            "task": "classify",
            "expected_length": "technical category",
        },
        {
            "id": 5,
            "text": "Data engineering involves building robust pipelines to extract, transform, and load data efficiently.",
            "task": "keywords",
            "expected_length": "3-5 keywords",
        },
        {
            "id": 6,
            "text": "The new product launch exceeded expectations with 10,000 users in the first week.",
            "task": "sentiment",
            "expected_length": "one word",
        },
        {
            "id": 7,
            "text": "Cloud computing, edge computing, and quantum computing represent different paradigms.",
            "task": "extract",
            "expected_length": "list",
        },
    ]

    df = pd.DataFrame(data)
    context.log.info(f"Loaded {len(df)} records for processing")

    # Log task distribution
    task_counts = df["task"].value_counts().to_dict()
    context.log.info(f"Task distribution: {task_counts}")

    return df


@asset(
    description="Process input data through Ollama LLM",
    retry_policy=RetryPolicy(max_retries=1, delay=3),
    group_name=LLM_CONNECTIVITY_TEST_GROUP_NAME,
)
def llm_processing(
    context: AssetExecutionContext,
    ollama: OllamaResource,
    data_processing: DataProcessingResource,
    available_models: Dict[str, Any],
    input_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Process input data through Ollama LLM with batch processing.

    Args:
        available_models: Model availability status
        input_data: DataFrame with text to process

    Returns:
        DataFrame with LLM responses and metadata
    """

    # Determine which model to use
    active_model = ollama.get_active_model(available_models)
    context.log.info(f"Using model: {active_model}")

    results = []
    total_records = len(input_data)

    # Create batches
    batches = data_processing.create_batches(input_data.to_dict("records"))
    context.log.info(f"Processing {total_records} records in {len(batches)} batches")

    # Process each batch
    for batch_idx, batch in enumerate(batches, 1):
        context.log.info(f"Processing batch {batch_idx}/{len(batches)}")

        # Prepare prompts for batch
        prompts = []
        for record in batch:
            prompt = data_processing.create_prompt(
                text=record["text"],
                task=record["task"],
                expected_length=record.get("expected_length", "concise"),
            )

            prompts.append(
                {
                    "id": record["id"],
                    "prompt": prompt,
                    "task": record["task"],
                    "original_text": record["text"],
                }
            )

        # Process batch through Ollama
        batch_results = ollama.batch_chat(
            prompts=prompts,
            model=active_model,
            system_prompt="You are a helpful assistant. Be concise and direct in your responses.",
        )

        # Add to results
        for result in batch_results:
            if result["success"]:
                results.append(
                    {
                        "id": result["id"],
                        "text": result["original_text"],
                        "task": result["task"],
                        "prompt": result["prompt"],
                        "response": result["response"],
                        "model": result["model"],
                        "tokens_input": result["tokens_input"],
                        "tokens_output": result["tokens_output"],
                        "total_tokens": result["total_tokens"],
                        "status": "success",
                    }
                )
            else:
                context.log.error(
                    f"Error processing ID {result['id']}: {result.get('error', 'Unknown error')}"
                )
                results.append(
                    {
                        "id": result["id"],
                        "text": result["original_text"],
                        "task": result["task"],
                        "error": result.get("error", "Unknown error"),
                        "status": "failed",
                    }
                )

    results_df = pd.DataFrame(results)

    # Save outputs if configured
    if data_processing.save_outputs:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"llm_results_{timestamp}.csv"
        output_path = data_processing.get_output_path(filename)

        results_df.to_csv(output_path, index=False)
        context.log.info(f"Saved results to {output_path}")

    # Log summary
    successful = len(results_df[results_df["status"] == "success"])
    context.log.info(f"Completed: {successful}/{total_records} successful")

    return results_df


@asset(
    description="Analyze and summarize LLM processing results",
    group_name=LLM_CONNECTIVITY_TEST_GROUP_NAME,
)
def processing_analytics(
    context: AssetExecutionContext,
    llm_processing: pd.DataFrame,
) -> MaterializeResult:
    """
    Generate analytics and insights from LLM processing results.

    Args:
        llm_processing: DataFrame with LLM processing results

    Returns:
        MaterializeResult with metadata and summary
        :param llm_processing:
        :param context:
    """

    total_records = len(llm_processing)
    successful = len(llm_processing[llm_processing["status"] == "success"])
    failed = total_records - successful
    success_rate = (successful / total_records * 100) if total_records > 0 else 0

    context.log.info(
        f"Analytics: {successful}/{total_records} successful ({success_rate:.1f}%)"
    )

    if successful == 0:
        context.log.error("No successful processing results to analyze")
        return MaterializeResult(
            metadata={
                "total_records": total_records,
                "successful": 0,
                "failed": failed,
                "success_rate": 0.0,
            }
        )

    # Filter to successful records
    success_df = llm_processing[llm_processing["status"] == "success"]

    # Token statistics
    total_input_tokens = success_df["tokens_input"].sum()
    total_output_tokens = success_df["tokens_output"].sum()
    total_tokens = total_input_tokens + total_output_tokens
    avg_tokens_per_request = total_tokens / successful if successful > 0 else 0

    # Task breakdown
    task_summary = (
        llm_processing.groupby("task")["status"].value_counts().unstack(fill_value=0)
    )

    # Response length statistics
    success_df["response_length"] = success_df["response"].str.len()
    avg_response_length = success_df["response_length"].mean()

    # Create detailed summary markdown
    summary_md = f"""
## LLM Processing Summary

### Overall Statistics
- **Total Records:** {total_records}
- **Successful:** {successful} ({success_rate:.1f}%)
- **Failed:** {failed}
- **Model Used:** {success_df['model'].iloc[0] if len(success_df) > 0 else 'N/A'}

### Token Usage
- **Total Tokens:** {total_tokens:,}
- **Input Tokens:** {total_input_tokens:,}
- **Output Tokens:** {total_output_tokens:,}
- **Average per Request:** {avg_tokens_per_request:.1f}

### Response Statistics
- **Average Response Length:** {avg_response_length:.0f} characters
- **Min Response Length:** {success_df['response_length'].min():.0f}
- **Max Response Length:** {success_df['response_length'].max():.0f}

### Task Breakdown
{task_summary.to_markdown() if not task_summary.empty else 'No task data'}

### Sample Results
"""

    # Add sample results (first 3 successful)
    for idx, row in success_df.head(3).iterrows():
        sample_text = (
            row["text"][:100] + "..." if len(row["text"]) > 100 else row["text"]
        )
        sample_response = (
            row["response"][:150] + "..."
            if len(row["response"]) > 150
            else row["response"]
        )

        summary_md += f"""
#### {row['task'].upper()} (ID: {row['id']})
- **Input:** {sample_text}
- **Output:** {sample_response}
- **Tokens:** {row['total_tokens']}
"""

    # Log key metrics
    context.log.info(f"Total tokens used: {total_tokens:,}")
    context.log.info(f"Average response length: {avg_response_length:.0f} chars")

    return MaterializeResult(
        metadata={
            "total_records": total_records,
            "successful": successful,
            "failed": failed,
            "success_rate": MetadataValue.float(success_rate),
            "total_tokens": MetadataValue.float(float(total_tokens)),
            "input_tokens": MetadataValue.float(float(total_input_tokens)),
            "output_tokens": MetadataValue.float(float(total_output_tokens)),
            "avg_tokens_per_request": MetadataValue.float(
                float(avg_tokens_per_request)
            ),
            "avg_response_length": MetadataValue.float(float(avg_response_length)),
            "summary": MetadataValue.md(summary_md),
            "task_breakdown": MetadataValue.md(
                task_summary.to_markdown() if not task_summary.empty else "No data"
            ),
        }
    )
