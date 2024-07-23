%run "./DSM_notebook"

# Keep the above RUN in a seperate code cell to avoid issue like:
# <Failed to parse %run command: string matching regex `\$[\w_]+' expected but `#' found>

# Define Key UUIDs 
lname_kid = ""
email_kid = ""


# Specify the table, column names to be tokenized with respective Key UUIDs.
# In this sample notebook, we have a precreated table as follows:
#  "employees.data" 
#       employee_id     bigint
#       fname           string
#       lname           string
#       email           string

# The sample function tokenizes the columns 'lname', 'email' and creates a new table called "tokenized_<table_name>" in the function insert_tokenizedData(). 
# Comment out calling insert_tokenizedData() from tokenize_col() in DSM_notebook if you not need another table to be created. 
tokenize_col("employees","data",["lname","email"],[lname_kid,email_kid])

# Specify the table, column names to be detokenized with respective Key UUIDs
detokenize_col("employees","tokenized_data",["lname","email"],[lname_kid,email_kid])



