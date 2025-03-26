from openai import OpenAI
import openai
import time


def process_row(
        openai_api_key: str,
        company: str,
        year:str,
        quarter: str,
        markdown_text:str
):
    """
    Calls OpenAI API to analyze financial reports and retries if a rate limit error occurs.

    Parameters:
    - openai_client: OpenAI client instance
    - company (str): Company name
    - year (str): Financial year
    - quarter (str): Financial quarter
    - markdown_text (str): Text content to analyze

    Returns:
    - str: Investment sentiment ('positive', 'neutral', or 'negative')
    """

    # the max token count is 40,000, so we need to truncate the Markdown text and leave some space for the prompt
    # we are truncating characters not token so this might be imperfect and sometime go wrong
    truncated_markdown = markdown_text[:20_000].strip()
    prompt = f"""
    This is the beginning of the 10-Q report for {company} in {year} Q{quarter}, expressed in markdown: {truncated_markdown}.
    As an expert financial analyst in the tech sector, you have to analyze this document and provide your investment sentiment.
    Please, respond only with the sentiment, not the markdown text: limit your response to one word, either 'positive', 'neutral', or 'negative'.
    """

    openai_client = OpenAI()
    max_retries = 5
    retry_delay = 5  # Initial retry delay in seconds

    for attempt in range(max_retries):
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert financial analyst in the tech sector."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content

        except openai.RateLimitError:
            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff (5s, 10s, 20s, etc.)
            print(f"Rate limit hit. Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)

    print("Max retries reached. Skipping this request.")
    return "Error: Rate limit exceeded"
