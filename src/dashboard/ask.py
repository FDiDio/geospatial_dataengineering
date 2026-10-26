"""
The page of the dashboard, app.py, where an agent answers questions about the weather in plain language: an
open model served by Ollama, on this computer or on Ollama Cloud, picks the tools and writes the SQL, and the
data of the explorer gives every number. Without Ollama the page says how to start it.
"""

import collections
import json
import math
import os
import sys
import time

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import agent, agent_eval

EXAMPLES = [
    "Quanti giorni sopra 30 °C ha avuto Milano nel luglio 2022?",
    "Che tempo farà domani a Roma?",
    "Quali sono state le temperature medie annue a Milano negli ultimi 10 anni?",
    "A che ora ha fatto più caldo a Siviglia il 14 luglio 2022?",
]
# What the agent has done, while it works on the next step
DONE = {
    "find_place": "Found the place",
    "query_days": "Queried the daily weather",
    "query_hours": "Queried the hourly weather",
    "cell_altitude": "Measured the altitude",
}
# Lines of a tool result shown before it scrolls
RESULT_LINES = 12
# Questions answered in an hour, to all the visitors together, by default with no limit: a public demo shares
# the quota of its Ollama Cloud key, and the Open-Meteo quota of its host
QUESTIONS_PER_HOUR = int(os.environ.get("AGENT_QUESTIONS_PER_HOUR", "0"))


@st.cache_resource
def asked() -> collections.deque[float]:
    """The times of the questions of the last hour, shared by the sessions of all the visitors."""
    return collections.deque()


def minutes_to_wait(times: collections.deque[float], now: float) -> int:
    """Minutes before a question can be answered within the questions of an hour, 0 when it can be now."""
    while times and times[0] <= now - 3600:
        times.popleft()
    if not QUESTIONS_PER_HOUR or len(times) < QUESTIONS_PER_HOUR:
        return 0
    return math.ceil((times[0] + 3600 - now) / 60)


def pretty(result: str) -> str:
    try:
        return json.dumps(json.loads(result), indent=2, ensure_ascii=False)
    except ValueError:
        return result


def show_step(step: agent.Step) -> None:
    """A tool call: the tool and its arguments, the SQL it ran and its result."""
    arguments = dict(step.arguments)
    sql = arguments.pop("sql", None)
    badge = ":red-badge[error]" if step.error else f":gray-badge[{step.seconds:.1f} s]"
    st.markdown(f"`{step.tool}` {badge} `{json.dumps(arguments, ensure_ascii=False)}`")
    if sql:
        st.code(sql, language="sql", wrap_lines=True)
    result = pretty(step.result)
    long = result.count("\n") > RESULT_LINES
    st.code(
        result,
        language="text" if step.error else "json",
        wrap_lines=True,
        height=24 * RESULT_LINES if long else "content",
    )


def how(answer: agent.Answer) -> str:
    """The label of the steps of an answer."""
    if not answer.steps:
        return "Answered without querying the data"
    calls = f"{len(answer.steps)} tool call{'s' if len(answer.steps) > 1 else ''}"
    return f"How I got it: {calls}, {answer.seconds:.1f} s, {answer.model}"


def show_check(answer: agent.Answer) -> None:
    """A warning on the numbers of an answer that the data did not give."""
    if answer.unverified:
        st.warning(
            f"Not in the data the agent read: {', '.join(answer.unverified)}. The model computed or made up these "
            "numbers: check them in **How I got it**.",
            icon=":material/warning:",
        )


def show_answer(answer: agent.Answer) -> None:
    with st.status(how(answer), state="complete"):
        for step in answer.steps:
            show_step(step)
    st.markdown(answer.text)
    show_check(answer)


def eval_score(model: str, think: bool) -> str | None:
    """The latest score of a model on the evaluation of the agent, if it ran."""
    results = agent_eval.latest_results()
    if results.empty:
        return None
    runs = results[(results["model"] == model) & (results["think"] == think)]
    if runs.empty:
        return None
    return f"Evaluation: {runs['passed'].sum()} of {len(runs)} questions right, {runs['seconds'].mean():.1f} s each"


st.title("Ask the weather")
st.caption(
    f"An open AI model{'' if agent.CLOUD else ', on this computer,'} answers questions about the weather of any "
    "place, from 1940 to the next two weeks. It looks every number up in the weather data: open **How I got it** "
    "to see how."
)

try:
    models = agent.available_models()
except Exception as error:  # Ollama is not installed or not running, or Ollama Cloud is out of reach
    if agent.CLOUD:
        st.error(f"Ollama Cloud does not answer: {error}", icon=":material/error:")
    else:
        st.info(
            f"The agent needs [Ollama](https://ollama.com) running at {agent.HOST}, with a model that calls tools: "
            "install it, then download the model with `ollama pull qwen3:8b`.",
            icon=":material/info:",
        )
    st.stop()
if not models:
    if agent.CLOUD:
        st.error(f"Ollama Cloud serves none of the models of AGENT_MODELS: {', '.join(agent.MODELS)}.")
    else:
        st.info("Ollama has no model yet: download one with `ollama pull qwen3:8b`.", icon=":material/info:")
    st.stop()

chat = st.session_state.setdefault("chat", [])
with st.sidebar:
    model = st.selectbox("Model", models, index=models.index(agent.MODEL) if agent.MODEL in models else 0)
    think = st.toggle(
        "Think before answering",
        help="The model reasons before each step: slower, sometimes better. gpt-oss always reasons.",
    )
    if score := eval_score(model, think):
        st.caption(score)
    st.button("New conversation", icon=":material/add_comment:", on_click=chat.clear)

# The input stays at the bottom of the page; read first, so that the examples go as soon as a question comes
question = st.chat_input("Ask about the weather of any place, from 1940 to the next two weeks")
question = question or st.session_state.pop("example", None)

for message in chat:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            show_answer(message["answer"])
        else:
            st.markdown(message["content"])

if not chat and not question:
    with st.container(horizontal=True, gap="small", key="examples"):
        for example in EXAMPLES:
            st.button(example, on_click=st.session_state.update, kwargs={"example": example})

if question and (wait := minutes_to_wait(asked(), time.time())):
    st.warning(
        f"This demo answers {QUESTIONS_PER_HOUR} questions an hour, to all its visitors together: ask again in "
        f"{wait} minute{'s' if wait > 1 else ''}.",
        icon=":material/hourglass_top:",
    )
elif question:
    if QUESTIONS_PER_HOUR:
        asked().append(time.time())
    history = [{"role": message["role"], "content": message["content"]} for message in chat]
    with st.chat_message("user"):
        st.markdown(question)
    with st.chat_message("assistant"):
        with st.status("Thinking…", expanded=True) as status:

            def on_step(step: agent.Step) -> None:
                status.update(label=f"{DONE.get(step.tool, step.tool)}, thinking…")
                show_step(step)

            try:
                answer = agent.ask(question, history, model=model, think=think, on_step=on_step)
            except Exception as error:  # the model is missing, cannot call tools, or Ollama stopped
                status.update(label="The model did not answer", state="error")
                st.error(str(error))
                st.stop()
            status.update(label=how(answer), state="complete", expanded=False)
        st.markdown(answer.text)
        show_check(answer)
    chat += [{"role": "user", "content": question}, {"role": "assistant", "content": answer.text, "answer": answer}]
