import json
import os
from openai import OpenAI
from .config import get_host, get_token, LLM_ENDPOINT
from .prompts import load_by_alias, PROMPT_NAME


def _get_llm_client() -> OpenAI:
    host = get_host()
    token = get_token()
    return OpenAI(base_url=f"{host}/serving-endpoints", api_key=token)


def _strip_markdown(text: str) -> str:
    """Strip markdown code blocks from LLM output."""
    clean = text.strip()
    if clean.startswith("```"):
        lines = clean.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        clean = "\n".join(lines).strip()
    return clean


def extract_with_prompt(parsed_text: str, prompt_template: str) -> dict:
    """Run extraction using a prompt template."""
    filled = prompt_template.replace("{{doc_text}}", parsed_text)
    client = _get_llm_client()

    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        messages=[
            {"role": "system", "content": "You are a precise data extraction assistant. Return only valid JSON."},
            {"role": "user", "content": filled}
        ],
        max_tokens=1000,
        temperature=0.0
    )

    result_text = response.choices[0].message.content
    clean_text = _strip_markdown(result_text)
    tokens = response.usage.total_tokens if response.usage else None

    try:
        result_json = json.loads(clean_text)
        return {"success": True, "data": result_json, "tokens": tokens}
    except json.JSONDecodeError:
        return {"success": False, "raw": result_text, "tokens": tokens}


def compare_prompts(parsed_text: str) -> dict:
    """Run extraction with production and staging prompts, return comparison."""
    prod = load_by_alias("production")
    staging = load_by_alias("staging")

    results = {"production": None, "staging": None}

    if "error" not in prod:
        prod_result = extract_with_prompt(parsed_text, prod["template"])
        results["production"] = {
            "version": prod["version"],
            "result": prod_result
        }

    if "error" not in staging:
        staging_result = extract_with_prompt(parsed_text, staging["template"])
        results["staging"] = {
            "version": staging["version"],
            "result": staging_result
        }

    # Compute diff
    if results["production"] and results["staging"]:
        prod_data = results["production"]["result"].get("data", {})
        stag_data = results["staging"]["result"].get("data", {})
        all_keys = set(list(prod_data.keys()) + list(stag_data.keys()))
        diff = {}
        for k in all_keys:
            pv = str(prod_data.get(k, "N/A"))
            sv = str(stag_data.get(k, "N/A"))
            diff[k] = {"production": pv, "staging": sv, "changed": pv != sv}
        results["diff"] = diff

    return results
