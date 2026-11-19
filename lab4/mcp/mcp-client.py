import asyncio
import json
import os
import re
import sys
from pathlib import Path
import traceback
import streamlit as st
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from openai import AsyncOpenAI


MCP_SERVER_PATH = Path(__file__).parent / "mcp_server.py"

SYSTEM_PROMPT = """Ти — інтелектуальний асистент для роботи з розподіленим сховищем даних.
Відповідай на запитання користувача ВИКЛЮЧНО на основі даних, отриманих через доступні MCP-інструменти сховища.
Правила:
1. Якщо не знаєш, які таблиці є в базі, спочатку виклич `get_tables`.
2. Для пошуку інформації використовуй інструменти читання (`read_records`, `read_records_by_id`, `read_record`).
3. Ніколи не вигадуй дані. Якщо записів або таблиці немає — чесно повідом про це.
4. У тексті відповіді обов'язково вказуй, з якої таблиці та за яким ключем (`id`) взято інформацію.
5. У відповіді явно пиши якими інструментами користувався"""


def clean_thinking_tags(text: str) -> str:
    """Прибирає службові теги роздумів <think>...</think> у моделей Qwen 3."""
    if not text:
        return ""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


async def run_agent_turn(
    chat_history: list[dict],
    base_url: str,
    api_key: str,
    model_name: str,
) -> tuple[str, list[dict]]:
    
    """Запускає сесію MCP, виконує цикл виклику інструментів та повертає відповідь і джерела."""
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[str(MCP_SERVER_PATH)],
        cwd=str(MCP_SERVER_PATH.parent),
    )

    used_sources: list[dict] = []
    # піднімає mcp_server.py як дочірній фоновий процес і відкриває 
    # два канали зв'язку (read_stream, write_stream) через стандартні потоки вводу/виводу (stdin/stdout)
    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            # Отримуємо список інструментів із mcp_server.py та конвертуємо під OpenAI-формат
            mcp_tools_resp = await session.list_tools()
            openai_tools = []
            for tool in mcp_tools_resp.tools:
                schema = tool.input_schema or {}
                props = schema.get("properties", {})

                cleaned_props = {
                    k: {
                        pk: pv
                        for pk, pv in v.items()
                        if pk in ("type", "description", "enum", "items")
                    }
                    for k, v in props.items()
                }

                parameters = {
                    "type": "object",
                    "properties": cleaned_props,
                }
                if schema.get("required"):
                    parameters["required"] = schema["required"]

                openai_tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": tool.name,
                            "description": tool.description or "",
                            "parameters": parameters,
                        },
                    }
                )

            # створює асинхронний HTTP-клієнт. Завдяки єдиному протоколу весь 
            # подальший код не знає і не залежить від того, чи запит іде на localhost:11434, чи на сервери Google
            client = AsyncOpenAI(base_url=base_url, api_key=api_key)

            messages = [{"role": "system", "content": SYSTEM_PROMPT}]
            for msg in chat_history:
                messages.append({"role": msg["role"], "content": msg["content"]})

            # Цикл ReAct / Tool Calling (до 8 кроків на один запит)
            # цикл міркувань агента з запобіжником від зациклення (на випадок, якщо локальна модель
            #  почне безкінечно викликати одну й ту саму функцію). Зазвичай вистачає 2–3 ітерацій: 
            # наприклад, на 1-му кроці модель викликає get_tables, на 2-му — read_records, 
            # а на 3-му формує фінальну текстову відповідь.
            for _ in range(8):
                response = await client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    tools=openai_tools,
                    temperature=0.1,
                )
                assistant_msg = response.choices[0].message
                # Спочатку ми додаємо в історію messages сам запит моделі на виклик функцій 
                # (role: "assistant"), щоб у контексті зберігся зв'язок між викликом та 
                # його результатом.
                if assistant_msg.tool_calls:

                    messages.append(
                        {
                            "role": "assistant",
                            "content": assistant_msg.content or "",
                            "tool_calls": [
                                {
                                    "id": tc.id,
                                    "type": "function",
                                    "function": {
                                        "name": tc.function.name,
                                        "arguments": tc.function.arguments,
                                    },
                                }
                                for tc in assistant_msg.tool_calls
                            ],
                        }
                    )
                    # Для кожного виклику аргументи парсяться з JSON-рядка у словник parsed_args і
                    #  передаються в await session.call_tool(tc.function.name, parsed_args), 
                    # який через stdio дьоргає відповідну функцію в mcp_server.py
                    #  (а та робить HTTP-запит до coordinator).
                    for tc in assistant_msg.tool_calls:
                        raw_args = tc.function.arguments
                        parsed_args = (
                            json.loads(raw_args)
                            if isinstance(raw_args, str) and raw_args.strip()
                            else (raw_args or {})
                        )

                        tool_result = await session.call_tool(tc.function.name, parsed_args)
                        output_text = "\n".join(
                            block.text for block in tool_result.content if hasattr(block, "text")
                        )
                        # Паралельно в список used_sources записується назва функції, передані 
                        # аргументи та сирий JSON-текст відповіді з бази — саме це потім виводиться 
                        # під повідомленням як доказ, звідки взято дані.
                        used_sources.append(
                            {
                                "tool": tc.function.name,
                                "args": parsed_args,
                                "result": output_text,
                            }
                        )
                        # Результат роботи інструменту повертається в messages із роллю 
                        # "role": "tool" і прив'язкою до tool_call_id, після чого цикл 
                        # робить наступну ітерацію.
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "content": output_text,
                            }
                        )
                # Коли модель готова відповідати (else:): tool_calls порожній,
                #  отже модель отримала всі потрібні дані з бази і згенерувала текст.
                # Функція вирізає теги <think> через clean_thinking_tags і повертає пару 
                # (final_answer, used_sources)
                else:
                    final_answer = clean_thinking_tags(assistant_msg.content or "")
                    return final_answer, used_sources

    return "Перевищено ліміт кроків виклику інструментів.", used_sources


# --- Інтерфейс Streamlit ---
st.set_page_config(page_title="MCP Storage Chatbot",  layout="centered")
st.title("Чатбот над розподіленим сховищем")

with st.sidebar:
    st.header("Налаштування LLM")
    provider = st.radio(
        "Режим роботи:",
        ["Локально (Ollama)", "Хмара (Google Gemini)"],
    )

    if provider == "Локально (Ollama)":
        base_url = "http://localhost:11434/v1"
        api_key = "ollama"
        model_name = st.text_input("Модель Ollama:", value="hf.co/unsloth/Qwen3.8-27B-GGUF:UD-Q3_K_XL")
        st.caption("Переконайтеся, що Ollama запущена локально.")
    else:
        base_url = "https://generativelanguage.googleapis.com/v1beta/openai/"
        api_key = st.text_input(
            "Gemini API Key:",
            value=os.getenv("GEMINI_API_KEY", ""),
            type="password",
        )
        model_name = st.text_input("Модель Gemini:", value="gemini-2.5-flash")
    # Вона очищає список st.session_state.messages і викликає st.rerun() — 
    # примусовий миттєвий перезапуск скрипту, щоб старі повідомлення одразу зникли з екрана.
    if st.button("Очистити історію чату"):
        st.session_state.messages = []
        st.rerun()
# спрацьовує лише один раз при першому відкритті сторінки і створює порожній список для повідомлень.
#  При наступних взаємодіях умова стає хибною, тому накопичена історія не стирається.
if "messages" not in st.session_state:
    st.session_state.messages = []

# Відображення історії повідомлень
# Оскільки Streamlit при кожній дії перезапускає скрипт з нуля, 
# цей цикл щоразу заново будує весь попередній чат на екрані
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]): #створює візуальний блок повідомлення з відповідною іконкою
        st.markdown(msg["content"]) # виводить текст повідомлення з підтримкою форматування (списків, таблиць, виділення коду)
        if msg.get("sources"): #перевіряє, чи збережений у цьому повідомленні список використаних MCP-інструментів (він є тільки у відповідей асистента, які зверталися до бази)
            with st.expander(f" Джерела даних (викликів MCP: {len(msg['sources'])})"):
                for idx, src in enumerate(msg["sources"], 1):
                    st.markdown(f"**{idx}. Інструмент:** `{src['tool']}`")
                    st.code(f"Аргументи: {json.dumps(src['args'], ensure_ascii=False)}", language="json")
                    st.code(f"Результат:\n{src['result']}", language="json")

# Обробка нового запиту
if prompt := st.chat_input("Запитайте щось про дані у сховищі..."): #малює поле вводу внизу екрана і повертає None у стані спокою, або рядок тексту в момент, коли користувач натиснув Enter.
    if provider == "Хмара (Google Gemini)" and not api_key:
        st.error("Введіть Gemini API Key у бічній панелі зліва.")
        st.stop()

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner(f"Звертаюся до {model_name} та MCP-сервера..."):
            try:
                answer, sources = asyncio.run(
                    run_agent_turn(
                        chat_history=st.session_state.messages,
                        base_url=base_url,
                        api_key=api_key,
                        model_name=model_name,
                    )
                )
                st.markdown(answer)
                if sources:
                    with st.expander(f" Джерела даних (викликів MCP: {len(sources)})"):
                        for idx, src in enumerate(sources, 1):
                            st.markdown(f"**{idx}. Інструмент:** `{src['tool']}`")
                            st.code(
                                f"Аргументи: {json.dumps(src['args'], ensure_ascii=False)}",
                                language="json",
                            )
                            st.code(f"Результат:\n{src['result']}", language="json")

                st.session_state.messages.append(
                    {"role": "assistant", "content": answer, "sources": sources}
                )
            except Exception as e:
                details = "".join(traceback.format_exception(e))
                st.error("Помилка під час виконання запиту:")
                st.code(details, language="python")
