import os
import json
from openai import OpenAI

def maybe_narrate(report, grok_api_key):
    try:
        client = OpenAI(
            api_key=grok_api_key,
            base_url="https://api.x.ai/v1"  # xAI Grok endpoint
        )
        prompt = f"""You are a forensic ecommerce analyst.
Summarize these audit findings professionally for a merchant owner.
Be concise, premium, factual, and avoid exaggeration.
Provide: Executive Summary, CFO Recommendations, Priority Ranking.
Data: {json.dumps(report['findings'], indent=2)}"""

        response = client.chat.completions.create(
            model="grok-2-latest",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=800
        )
        narrative = response.choices[0].message.content
        report["executive_narrative"] = narrative
    except Exception as e:
        report["executive_narrative"] = f"AI narration failed: {str(e)}"
    return report
