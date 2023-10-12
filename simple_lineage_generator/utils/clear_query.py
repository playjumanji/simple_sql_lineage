import re


def clean_query(query):
    # Remove new lines
    query = query.replace("\n", " ")
    # Remove multiple spaces
    query = re.sub(" +", " ", query)
    return query
