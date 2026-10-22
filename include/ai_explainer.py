"""
include/ai_explainer.py
-----------------------
AI-powered Data Quality Explainer for the Data Pipeline to ML Model project.
Uses LangGraph and OpenAI-compatible LLMs (including NVIDIA NIM, Azure OpenAI,
and standard OpenAI) to generate:
  1. Plain-English root cause analysis of validation & quarantine failures.
  2. Stakeholder-friendly Slack alert message with key metrics and actionable next steps.

Supported LLM Providers (configured via Airflow Variables or Environment Variables):
  - NVIDIA NIM (Recommended):
      Airflow Variable / Env: NVIDIA_API_KEY
      Endpoint: https://integrate.api.nvidia.com/v1 (or NVIDIA_BASE_URL)
      Model: meta/llama-3.3-70b-instruct (or NVIDIA_MODEL)
  - Azure OpenAI (Upstream Scaffold):
      Airflow Variables / Env: AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT
  - OpenAI:
      Airflow Variable / Env: OPENAI_API_KEY, OPENAI_MODEL

Falls back gracefully to a deterministic rule-based template if no API keys are provided
or if packages/network are unavailable, guaranteeing 100% pipeline reliability.
"""

import os
import json
import logging
from typing import TypedDict, Optional, Dict, Any

logger = logging.getLogger("airflow.task")


# ── Shared LangGraph Alert State ─────────────────────────────────────────────

class AlertState(TypedDict):
    pipeline_name: str
    summary: dict
    failed_count: int
    passed_count: int
    checked_at: str
    root_cause: str
    slack_message: str


# ── LLM Client Resolver ──────────────────────────────────────────────────────

def _get_llm(temperature: float = 0):
    """
    Resolves the LLM client based on configured Airflow Variables or environment variables.
    Checks providers in order:
      1. NVIDIA NIM (NVIDIA_API_KEY)
      2. Azure OpenAI (AZURE_OPENAI_API_KEY)
      3. Standard OpenAI (OPENAI_API_KEY)
    Returns a LangChain-compatible chat model or None.
    """
    def _get_val(key: str, default: Optional[str] = None) -> Optional[str]:
        try:
            from airflow.models import Variable
            val = Variable.get(key, default_var=None)
            if val is not None and str(val).strip():
                return str(val).strip()
        except Exception:
            pass
        return os.getenv(key, default)

    # ── 1. Check NVIDIA NIM ────────────────────────────────────────────────────
    nvidia_key = _get_val("NVIDIA_API_KEY")
    if nvidia_key:
        base_url = _get_val("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
        model = _get_val("NVIDIA_MODEL", "meta/llama-3.3-70b-instruct")
        try:
            from langchain_openai import ChatOpenAI
            logger.info(f"[AI Explainer] Using NVIDIA NIM provider with model {model}")
            return ChatOpenAI(
                model=model,
                api_key=nvidia_key,
                base_url=base_url,
                temperature=temperature,
                timeout=15,
            )
        except Exception as e:
            logger.warning(f"[AI Explainer] Failed initializing ChatOpenAI for NVIDIA NIM: {e}")

    # ── 2. Check Azure OpenAI (Upstream Default) ──────────────────────────────
    azure_key = _get_val("AZURE_OPENAI_API_KEY")
    azure_endpoint = _get_val("AZURE_OPENAI_ENDPOINT")
    if azure_key and azure_endpoint:
        deployment = _get_val("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
        api_version = _get_val("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
        try:
            from langchain_openai import AzureChatOpenAI
            logger.info(f"[AI Explainer] Using Azure OpenAI provider with deployment {deployment}")
            return AzureChatOpenAI(
                azure_deployment=deployment,
                azure_endpoint=azure_endpoint,
                api_key=azure_key,
                api_version=api_version,
                temperature=temperature,
                timeout=15,
            )
        except Exception as e:
            logger.warning(f"[AI Explainer] Failed initializing AzureChatOpenAI: {e}")

    # ── 3. Check Standard OpenAI ──────────────────────────────────────────────
    openai_key = _get_val("OPENAI_API_KEY")
    if openai_key:
        openai_model = _get_val("OPENAI_MODEL", "gpt-4o-mini")
        try:
            from langchain_openai import ChatOpenAI
            logger.info(f"[AI Explainer] Using OpenAI provider with model {openai_model}")
            return ChatOpenAI(
                model=openai_model,
                api_key=openai_key,
                temperature=temperature,
                timeout=15,
            )
        except Exception as e:
            logger.warning(f"[AI Explainer] Failed initializing standard ChatOpenAI: {e}")

    return None


# ── Node 1: Analyze Root Cause ───────────────────────────────────────────────

def analyse_node(state: AlertState) -> AlertState:
    pipeline_name = state.get("pipeline_name", "Data Pipeline")
    summary = state.get("summary", {})
    failed_count = state.get("failed_count", 0)
    passed_count = state.get("passed_count", 0)
    checked_at = state.get("checked_at", "Unknown")

    llm = _get_llm(temperature=0)

    if llm:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            system_prompt = (
                "You are a Senior Forward Deployed Data Engineer (FDE) and Data Quality Analyst. "
                "Analyze pipeline validation reports, quarantine logs, and DQ summaries. "
                "Provide an objective, non-technical root cause analysis explaining what went wrong, "
                "which data dimensions/columns failed, and the potential business impact on downstream reporting/ML."
            )
            user_prompt = f"""
Pipeline Name: {pipeline_name}
Validation Ran At: {checked_at}
Passed Records: {passed_count}
Failed / Quarantined Records: {failed_count}

Validation Issue Summary:
{json.dumps(summary, indent=2, default=str)}

Write a concise root cause analysis (3-4 sentences):
1. Exactly what data anomalies or rule violations occurred.
2. Which columns/files are affected.
3. Why quarantining these rows protects finance dashboards and downstream ML forecasts.
"""
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
            response = llm.invoke(messages)
            root_cause = response.content.strip()
        except Exception as e:
            logger.warning(f"[AI Explainer] LLM invocation failed in analyse_node: {e}. Using deterministic fallback.")
            llm = None

    if not llm:
        # Deterministic rule-based template
        lines = []
        for key, issues in summary.items():
            if isinstance(issues, list):
                for issue in issues:
                    lines.append(f"* {key}: {issue}")
            elif isinstance(issues, dict):
                for subkey, count in issues.items():
                    lines.append(f"* {key} ({subkey}): {count} occurrences")
            else:
                lines.append(f"* {key}: {issues}")

        issues_formatted = "\n".join(lines) if lines else "* Data quality anomaly detected across input files."
        root_cause = (
            f"Quality validation for {pipeline_name} at {checked_at} identified {failed_count} failing record(s).\n"
            f"{issues_formatted}\n"
            f"These records violate completeness, validity, or referential integrity constraints "
            f"and have been safely segregated into the quarantine store to prevent corrupting downstream analytics."
        )

    state["root_cause"] = root_cause
    return state


# ── Node 2: Compose Stakeholder Slack Alert ──────────────────────────────────

def compose_node(state: AlertState) -> AlertState:
    pipeline_name = state.get("pipeline_name", "Data Pipeline")
    failed_count = state.get("failed_count", 0)
    passed_count = state.get("passed_count", 0)
    root_cause = state.get("root_cause", "")
    checked_at = state.get("checked_at", "Unknown")

    llm = _get_llm(temperature=0.3)

    if llm:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            system_prompt = (
                "You are an Operations Communications Assistant. Convert technical data quality "
                "findings into a crisp, professional Slack card for business and operations stakeholders."
            )
            user_prompt = f"""
Pipeline: {pipeline_name}
Timestamp: {checked_at}
Passed Records: {passed_count}
Quarantined Records: {failed_count}

Root Cause Analysis:
{root_cause}

Generate a concise Slack alert message (under 6 lines):
- Start with an alert status header emoji (e.g., 🚨 or ⚠️).
- Include execution timestamp and clean vs quarantined row counts.
- Summarize the root cause in 2 punchy bullet points.
- Include a clear call to action (check quarantine folder/dashboard).
"""
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
            response = llm.invoke(messages)
            slack_message = response.content.strip()
        except Exception as e:
            logger.warning(f"[AI Explainer] LLM invocation failed in compose_node: {e}. Using deterministic fallback.")
            llm = None

    if not llm:
        # Deterministic Slack card template (Slack markup format)
        slack_message = (
            f":rotating_light: *{pipeline_name} — Data Quality Alert*\n"
            f"Run Time: `{checked_at}`\n"
            f":white_check_mark: Clean Records Loaded: *{passed_count:,}*\n"
            f":x: Records Quarantined: *{failed_count:,}*\n"
            f"*Root Cause Summary:*\n{root_cause}\n"
            f"*Action:* Inspect `include/quarantine/` and `dq_results` audit table for triage."
        )

    state["slack_message"] = slack_message
    return state


# ── Build LangGraph / Fallback Runner ────────────────────────────────────────

def build_graph():
    """
    Compiles a LangGraph StateGraph if langgraph is available,
    otherwise returns a pure Python runner function with identical semantics.
    """
    try:
        from langgraph.graph import StateGraph, END
        graph = StateGraph(AlertState)
        graph.add_node("analyse", analyse_node)
        graph.add_node("compose", compose_node)
        graph.set_entry_point("analyse")
        graph.add_edge("analyse", "compose")
        graph.add_edge("compose", END)
        return graph.compile()
    except Exception as e:
        logger.info(f"[AI Explainer] LangGraph compiled runner fallback: {e}")
        class SimpleGraphRunner:
            def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
                s = analyse_node(state)  # type: ignore
                s = compose_node(s)      # type: ignore
                return s
        return SimpleGraphRunner()


def _safe_print(text: str) -> None:
    try:
        print(text)
    except Exception:
        try:
            print(text.encode("ascii", errors="replace").decode("ascii"))
        except Exception:
            pass


def explain_failures(gx_result: dict) -> str:
    """
    Public function compatible with upstream Meridian pipeline GX format.
    gx_result expects:
      {
         "summary": dict,
         "failed_count": int,
         "passed_count": int,
         "checked_at": str
      }
    """
    failed_count = gx_result.get("failed_count", 0)
    if failed_count == 0:
        return ""

    graph = build_graph()
    final_state = graph.invoke({
        "pipeline_name": "Meridian Data Pipeline",
        "summary": gx_result.get("summary", {}),
        "failed_count": failed_count,
        "passed_count": gx_result.get("passed_count", 0),
        "checked_at": gx_result.get("checked_at", "Unknown"),
        "root_cause": "",
        "slack_message": "",
    })

    _safe_print(f"\n[AI Explainer] Root Cause Analysis:\n{final_state['root_cause']}")
    _safe_print(f"\n[AI Explainer] Slack Message:\n{final_state['slack_message']}\n")
    return final_state["slack_message"]


def explain_quarantine(
    quarantine_summary: dict,
    pipeline_name: str = "NammaMart Quick Commerce Pipeline",
    passed_count: int = 0,
    failed_count: int = 0,
    checked_at: Optional[str] = None,
) -> str:
    """
    Public function designed for NammaMart ETL & ELT quarantine reporting.
    Accepts quarantine metrics, runs the LangGraph AI explainer, and returns
    the generated Slack alert text.
    """
    if failed_count == 0 and not quarantine_summary:
        return ""

    if not checked_at:
        import pendulum
        checked_at = pendulum.now("Asia/Kolkata").to_datetime_string()

    graph = build_graph()
    final_state = graph.invoke({
        "pipeline_name": pipeline_name,
        "summary": quarantine_summary,
        "failed_count": failed_count,
        "passed_count": passed_count,
        "checked_at": checked_at,
        "root_cause": "",
        "slack_message": "",
    })

    _safe_print(f"\n[AI Explainer] Pipeline: {pipeline_name}")
    _safe_print(f"[AI Explainer] Root Cause:\n{final_state['root_cause']}")
    _safe_print(f"[AI Explainer] Slack Alert:\n{final_state['slack_message']}\n")
    return final_state["slack_message"]
