import logging
import os

import dotenv
import pandas as pd
import streamlit as st
from helper import analyze_data

dotenv.load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

logging.info("Database url:", DATABASE_URL)


def main():
    st.title("Pageviews Analysis")

    # Perform analysis (this function should use the PostgresHook and query the data)
    result = analyze_data(DATABASE_URL)

    company = result[0]

    st.write(
        f"The company with the highest pageviews is: {company[0].capitalize()} with {company[1]} pageviews.")

    # plot a graph of the data
    result = dict(result)

    # extract the companies and the pag views
    companies = [company.capitalize() for company in result.keys()]
    pageviews = result.values()

    result_df = pd.DataFrame({
        'Companies': companies,
        'Pageviews': pageviews
    })

    # plot a bar chart
    st.bar_chart(result_df.set_index('Companies'))


if __name__ == "__main__":
    main()
