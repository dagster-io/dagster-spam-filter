Every product that allows users to send messages to each other quickly realizes that spam is a real problem, even at modest scale. When sites allow malicious users to send spam, user trust quickly erodes and they start to leave for competitors.

Effective spam filtering, that hides spam messages while letting legitimate messages through, is an essential part of email or messaging systems.

Spam filters use a number of tricks to do this, but one of the most common is a simple model known as a _[Bag of Words](https://en.wikipedia.org/wiki/Bag-of-words_model)_. This approach builds a model based on the words in previous messages, and whether each word is more common in spam or non-spam messages.

In this walkthrough, we’ll build a simple model with Dagster and expose it with a [Flask API](https://python-data-science.readthedocs.io/en/latest/flask.html) endpoint that can be used to test new messages. Then we’ll create a job that integrates new messages to improve the model over time and redeploys the updated model.

# What we’re building

We’re building a simple (yet powerful) spam filter built on a _Bag of Words_ model. We’ll seed the data with [SMS Spam Collection Dataset](https://www.kaggle.com/datasets/uciml/sms-spam-collection-dataset) from Kaggle and base our approach on Youtuber[ Ritvikmath’s spam filter](https://www.youtube.com/watch?v=VQZxLPEdIpE&ab_channel=ritvikmath). This approach takes lists of spam and non-spam messages, looks at each word in those messages, and determines whether the presence of each word suggests the message is or is not spam.

We’ll take this simple approach and modify it by adding messages sent by our own users, and on a monthly schedule we’ll rebuild the model, compare it against our previous month’s model, and promote the new model to production if it scores more highly than the prior month’s.

The live model will then be surfaced via a simple Flask API endpoint that accepts a message body as input and returns whether or not it’s flagged as spam.

Our workflow will create the following seven assets. The final four assets will then be saved to a JSON file where they’ll be accessible to the API.

<center>
  <figure>
    <img
      src="/posts/dagster-spam-filter/spam-filter-asset-graph-min.png"
      alt="An asset graph used to detect spam messages to improve data quality."
      width="1908"
      height="1228"
    />
    <figcaption style={{ textAlign: 'center' }}>
      An Dagster asset graph showing the steps in detecting spam messages to improve data quality.
    </figcaption>
  </figure>
</center>

<aside className="rounded-3xl text-center">
  <span className="inline-block px-2 pb-2 align-middle">
    <svg width="26" height="26" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
      <g clip-path="url(#clip0_630_11)">
        <path
          fill-rule="evenodd"
          clip-rule="evenodd"
          d="M11.9642 0C5.34833 0 0 5.5 0 12.3042C0 17.7432 3.42686 22.3472 8.18082 23.9767C8.77518 24.0992 8.9929 23.712 8.9929 23.3862C8.9929 23.101 8.97331 22.1232 8.97331 21.1045C5.64514 21.838 4.95208 19.6378 4.95208 19.6378C4.41722 18.2118 3.62473 17.8452 3.62473 17.8452C2.53543 17.0915 3.70408 17.0915 3.70408 17.0915C4.91241 17.173 5.54645 18.3545 5.54645 18.3545C6.61592 20.2285 8.33927 19.699 9.03257 19.373C9.13151 18.5785 9.44865 18.0285 9.78539 17.723C7.13094 17.4377 4.33812 16.3785 4.33812 11.6523C4.33812 10.3078 4.81322 9.20775 5.56604 8.35225C5.44727 8.04675 5.03118 6.7835 5.68506 5.09275C5.68506 5.09275 6.69527 4.76675 8.97306 6.35575C9.94827 6.08642 10.954 5.9494 11.9642 5.94825C12.9744 5.94825 14.0042 6.091 14.9552 6.35575C17.2332 4.76675 18.2434 5.09275 18.2434 5.09275C18.8973 6.7835 18.481 8.04675 18.3622 8.35225C19.1349 9.20775 19.5904 10.3078 19.5904 11.6523C19.5904 16.3785 16.7976 17.4172 14.1233 17.723C14.5592 18.11 14.9353 18.8432 14.9353 20.0045C14.9353 21.6545 14.9158 22.9787 14.9158 23.386C14.9158 23.712 15.1337 24.0992 15.7278 23.977C20.4818 22.347 23.9087 17.7432 23.9087 12.3042C23.9282 5.5 18.5603 0 11.9642 0Z"
          fill="#24292F"
        />
      </g>
      <defs>
        <clipPath id="clip0_630_11">
          <rect width="24" height="24" fill="white" />
        </clipPath>
      </defs>
    </svg>
  </span>
  Want to skip right to the code? You can follow along or clone the project. <a href="https://github.com/dagster-io/dagster-spam-filter">It’s all on GitHub</a>.
</aside>



# Setup

Start by building the default[ Dagster skeleton project](https://docs.dagster.io/getting-started/create-new-project).


```python
pip install dagster
dagster project scaffold --name spam-filter
pip install -e ".[dev]"
```


Next, add the extra dependencies used in this project. These will allow us to build the _Bag of Words_ model and serve the API endpoint.


```python
pip install dagster-duckdb~=0.20 dagster-duckdb-pandas~=0.20 Flask~=2.3 pandas~=2.0 numpy~=1.25 scikit-learn~=1.3
```


# Integrating the SMS spam dataset

Our first asset will read the sample spam messages from a file and convert them into a Pandas DataFrame.

You can download the spam dataset from Kaggle or [this project’s GitHub](https://github.com/dagster-io/dagster-spam-filter/blob/main/spam.csv). This file contains example SMS messages where spam messages are marked “spam” and non-spam messages are marked “ham.”


<div style={{overflowX: 'scroll'}} className="text-xs md:text-lg">
  <table>
    <tr style={{border: '1px solid #ccc'}}>
      <td align="center">
        V1
      </td>
      <td align="center" style={{borderLeft: '1px solid #ccc'}}>
        V2
      </td>
    </tr>
    <tr style={{border: '1px solid #ccc'}}>
      <td className="p-4" style={{borderLeft: '1px solid #ccc'}}>
        ham
      </td>
      <td className="p-4" style={{borderLeft: '1px solid #ccc'}}>"Go until jurong point, crazy.. Available only in bugis n great world la e buffet... Cine there got amore wat..."</td>
    </tr>
    <tr style={{border: '1px solid #ccc'}}>
      <td className="p-1 sm:p-2 md:p-4">
        ham
      </td>
      <td className="p-4" style={{borderLeft: '1px solid #ccc'}}>
        Ok lar... Joking wif u oni...
      </td>
    </tr>
    <tr style={{border: '1px solid #ccc'}}>
      <td className="p-1 sm:p-2 md:p-4">
        spam
      </td>
      <td className="p-4" style={{borderLeft: '1px solid #ccc'}}>
        Free entry in 2 a wkly comp to win FA Cup final tkts 21st May 2005. Text FA to 87121 to receive entry question(std txt rate)T&C's apply 08452810075over18's
      </td>
    </tr>
    <tr style={{border: '1px solid #ccc'}}>
      <td className="p-1 sm:p-2 md:p-4">
        ham
      </td>
      <td className="p-4" style={{borderLeft: '1px solid #ccc'}}>
        U dun say so early hor... U c already then say...
      </td>
    </tr>
  </table>
</div>

The asset will read the file contents, rename the relevant columns to “spam” and “text,” and convert spam/ham to boolean values.


```python
from dagster import asset
import pandas as pd

@asset
def sms_spam_examples() -> pd.DataFrame:
    spam_df = pd.read_csv("./spam.csv", encoding="ISO-8859-1", usecols=["v1", "v2"])
    spam_df = spam_df.rename(columns={"v1": "spam", "v2": "text"})
    spam_df["spam"] = spam_df["spam"] == "spam"
    return spam_df
```


We’ve defined our first asset, now we need to make it accessible to the Dagster UI. We’ll do this by adding it to the Dagster Definition object. Think of Definition as the Dagster global scope object, where everything it’s aware of can be viewed and operated on by Dagster.

We’ll also define an "io_manager" resource. Setting the base directory to "data" tells Dagster to put working assets into a "data" directory.


```python
# __init__.py
from dagster import (
    load_assets_from_modules,
    FilesystemIOManager,
)

from . import assets
all_assets = load_assets_from_modules([assets])

io_manager = FilesystemIOManager(
    base_dir="data",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": io_manager,
    },
)
```



# Building the spam model

For our first pass at a spam model, we’ll build the _Bag of Words_ model based only on the SMS spam dataset. Later, we’ll integrate user messages from our application.

Beyond the Dagster code, we won’t dig into the specifics of the model code. If you’re curious about how to build a _Bag of Words_ model from scratch, check out[ Ritvikmath’s excellent YouTube video](https://www.youtube.com/watch?v=VQZxLPEdIpE&ab_channel=ritvikmath).

The following asset uses Dagster’s "multi_asset" to create multiple output assets. spam_model builds training and testing DataFrames as well as spam and non-spam bags of words. Creating multiple outputs may seem like overkill, but it lets us monitor each asset individually in the Dagster UI.


```python
# assets.py

from collections import Counter
from dagster import AssetOut, multi_asset
from sklearn.model_selection import train_test_split
...

@multi_asset(
    outs={
        "spam_bag_of_words": AssetOut(),
        "non_spam_bag_of_words": AssetOut(),
        "spam_training_dataframe": AssetOut(),
        "spam_testing_dataframe": AssetOut(),
    }
)
def spam_model(sms_spam_examples: pd.DataFrame):
    def preprocess_text(text: str) -> str:
        return text.lower().translate(str.maketrans("", "", string.punctuation))

    def calculate_word_frequencies(words: list, shared_words: set) -> dict:
        word_counts = Counter(words)
        total_words = len(words)
        return {word: word_counts[word] / total_words for word in shared_words}

    spam_dataset=sms_spam_examples
    spam_dataset["text"] = spam_dataset["text"].apply(preprocess_text)

    spam_dataset = spam_dataset.sample(frac=1)

    train_data, test_data = train_test_split(spam_dataset, test_size=0.3)

    spam_training_words = " ".join(train_data[train_data["spam"]].text).split()
    non_spam_training_words = " ".join(train_data[~train_data["spam"]].text).split()

    shared_words = set(spam_training_words).intersection(set(non_spam_training_words))
    spam_bag_of_words = calculate_word_frequencies(spam_training_words, shared_words)
    non_spam_bag_of_words = calculate_word_frequencies(
        non_spam_training_words, shared_words
    )

    return spam_bag_of_words, non_spam_bag_of_words, train_data, test_data
```


# Scoring the new model

Once our model exists, we can test it against the test dataset. We’ll calculate the true positive rate (spam messages accurately identified) and false positive rate (non-spam identified incorrectly identified as spam), allowing us to compare the new model with previously created models.

This asset will use all of the assets created by the spam_model function. model_score returns a dictionary of values rather than a multi_asset because the values are used for a single purpose and are only passed to one downstream asset. This contrasts with spam_model, where the assets created don’t move as a single unit. As we’ll see with the model_file asset created shortly, it only makes use of two of the assets created by spam_model.


```python
# asset
import numpy as np

...

@asset
def model_score(
    spam_bag_of_words: Dict,
    non_spam_bag_of_words: Dict,
    spam_training_dataframe: pd.DataFrame,
    spam_testing_dataframe: pd.DataFrame,
) -> Dict:
    def get_prediction_rate(predictions: pd.Series, true_values: pd.Series) -> float:
        return np.sum(predictions & true_values) / np.sum(true_values)

    def get_spam_probability(text: str) -> float:
        return spam.compare_text_to_bow(
            text.split(),
            spam_bag_of_words=spam_bag_of_words,
            non_spam_bag_of_words=non_spam_bag_of_words,
            percent_spam=percent_spam,
        )
```


We’ll need to create the file spam.py as well to store compare_text_to_bow(), which determines whether a piece of text is spam or not. This is placed in a standalone file, where it’s also accessible to our API.


```python
# spam.py

import numpy as np

def compare_text_to_bow(text, spam_bag_of_words, non_spam_bag_of_words, percent_spam):
    valid_words = [word for word in text if word in spam_bag_of_words]

    spam_probs, non_spam_probs = [], []
    for word in valid_words:
        spam_probs.append(spam_bag_of_words[word])
        non_spam_probs.append(non_spam_bag_of_words[word])

    spam_score = sum(map(np.log, spam_probs)) + np.log(percent_spam)
    non_spam_score = sum(map(np.log, non_spam_probs)) + np.log(1 - percent_spam)

    return bool(non_spam_score < spam_score)
```



# Tracking models in the database

We have all of the necessary pieces to create and score our spam filter model, and our next step is to save the model weights and create the database table to keep track of the current model to use. The first time we build the model, the model is automatically set to “active”, meaning it’s used in our spam filter. In the future, every time the model is built, it will be compared to the previous model and only promoted to production if performance exceeds the current model’s.

Before working in Dagster, we need to create an application database to keep track of the current model. Our API will be able to query this database to find the current model data, which is why we need to do more than create a Dagster asset that returns a model value.

A production database would likely be an RDBMS like Postgres or MySQL, but we’ll use DuckDB. Since our project will need two tables (“models” and, later, “messages”), let’s create both now.

Create the file “database.py” inside the spam_filter directory. This will create the database in your project’s root directory, which will be accessible to the API.


```python
# database.py

import duckdb


def create_database():
    conn = duckdb.connect("./database.duckdb")

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS messages (
        spam BOOLEAN,
        body VARCHAR
    );
    """
    )

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS models (
        path VARCHAR,
        status VARCHAR,
        true_positive_rate FLOAT,
        false_positive_rate FLOAT
    );
    """
    )


if __name__ == "__main__":
    create_database()
    conn = duckdb.connect("./database.duckdb")
    tables = conn.execute("SHOW TABLES").fetchall()
    print(tables)
```

Run “python database.py” from the command line to create the DuckDB database and tables.

It’s necessary to create a Dagster resource so that the database is also accessible within our workflow. The following resource looks confusing, but it serves two (hopefully) straightforward purposes.

Database.query() is a standalone method that connects to the database, runs an SQL query, and returns the responses as a Pandas DataFrame. We’ll use this method to look up individual records in the database.

When it’s time to add a new active record to the database, we need to run two separate commands — updating the previous active record to inactive and adding a new active record — as a single transaction. The Database.\_\_enter\_\_() and Database.\_\_exit\_\_() methods allow us to use the database resource as a context manager. This lets us create a connection and then call Database.execute() as many times as we want before committing all of the commands at once.

If you’re unfamiliar with Python’s context managers, take a look at the[ official Python documentation](https://docs.python.org/3/reference/datamodel.html#object.__enter__).


```python
import json
import os

import duckdb
import pandas as pd
from dagster import ConfigurableResource
from duckdb import DuckDBPyConnection
from pydantic import PrivateAttr


class Database(ConfigurableResource):
    path: str
    _conn: DuckDBPyConnection = PrivateAttr()

    def __enter__(self):
        self._conn = duckdb.connect(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()

    def execute(self, body):
        if self._conn is None:
            raise Exception(
                "Database connection not initialized. Use the object within a 'with' block."
            )

        self._conn.execute(body)

    def query(self, body: str):
        with duckdb.connect(self.path) as conn:
            result = conn.query(body)
            if result is None:
                return pd.DataFrame()
            else:
                return result.to_df()
```

Make this new resource accessible to Dagster by adding it to the Definition object.

```python
# __init__.py
from dagster import (
    Definitions
)

...
defs = Definitions(
    ...
    resources={
	   ...
        "database": Database(path="./database.duckdb"),
    },
)
```

## Look for pre-built integrations before writing your own resources

We defined a custom Database resource, but this often isn’t necessary. Dagster has dozens of existing integrations, including pre-built resources, that you can use out of the box. You can find these on the Dagster [integrations page](https://dagster.io/integrations).

We built this to illustrate how to create a context manager that works on any database type. If we knew we wanted to use DuckDB in both dev and prod, we could have used Dagster’s [DuckDB integration](https://docs.dagster.io/integrations/duckdb/using-duckdb-with-dagster#step-2-create-tables-in-duckdb).


# Saving models to a file

In addition to saving a record of our new model, we need to save the actual model weights to a file accessible to our API code. This means defining a new resource for storing our model file.

ModelStorage.setup_for_execution() will only be executed one time when Dagster is initialized, so it’s the perfect place to make sure our model storage directory exists. The .write() and .read() methods convert data to/from JSON.


```python
# resources.py
import json
import os

class ModelStorage(ConfigurableResource):
    dir: str

    def setup_for_execution(self, context) -> None:
        os.makedirs(self.dir, exist_ok=True)

    def write(self, filename, data):
        with open(f"{self.dir}/{filename}", "w") as f:
            json.dump(data, f)

    def read(self, filename):
        with open(f"{self.dir}/{filename}", "r") as f:
            return json.load(f)
```


Like our prior resources, we need to make ModelStorage accessible to Dagster.


```python
# __init__.py

from dagster import (
    Definitions
)

from .resources import ModelStorage
...
defs = Definitions(
    ...
    resources={
	   ...
        "model_storage": ModelStorage(dir="./weights"),
    },
)
```

# Connecting our model with the new resources

It’s finally time to save our model to a file that can be accessed by the API and create a database record if the model improves on existing models.

The following asset does a few key things. We’ll address the high-level purpose of the asset and then look at the Dagster-specific components.


1. Fetches the record of the currently active model from the database.
2. If no active record currently exists, or if the new model has more true positives and fewer false positives, proceed to save the new model.
3. Creates a file to store the new model.
4. Updates the existing database record to set it to “inactive” and creates a record for the new model. Note that we can use the database object as a context manager and call conn.execute() multiple times due to how we defined the Database resource.

```python
# asset.py

@asset
def model_file(
    model_score: Dict,
    spam_bag_of_words: Dict,
    non_spam_bag_of_words: Dict,
    database: Database,
    model_storage: ModelStorage,
):
    df = database.query("SELECT * FROM models WHERE status = 'active'")
    if (
        df.empty
        or (df.iloc[0].true_positive_rate <= model_score["true_positives"])
        and (df.iloc[0].false_positive_rate >= model_score["false_positives"])
    ):
        filename = f"{int(time.time())}.json"
        model_storage.write(
            filename,
            {
                "spam_bow": spam_bag_of_words,
                "non_spam_bow": non_spam_bag_of_words,
                "percent_spam": model_score["percent_spam"],
            },
        )
        with database as conn:
            conn.execute(
                "UPDATE models SET status = 'inactive' WHERE status = 'active'"
            )
            conn.execute(
                f"""
                INSERT INTO models (path, status, true_positive_rate, false_positive_rate) 
                VALUES ('{filename}', 'active', {model_score["true_positives"]}, {model_score["false_positives"]})
                """
            )
```


## How score_and_save_model is called by a Dagster job

To call score_and_save_model, we need to add it to a “job,” which is Dagster’s internal term for a pipeline. There are a few ways to define a job, but we’ll do it directly in our project’s \_\_init\_\_.py file.


```python
# __init__.py

from dagster import (
    define_asset_job,
)
...
create_spam_model_job = define_asset_job(name="create_spam_model_job")
```


define_asset_job grabs all of the assets in our directory and creates a job based on the asset graph.

In order to run this job on a recurring basis, we’ll need to create a schedule and pass the job and schedule objects to Dagster’s Definition object, as we did with the resources.


```python
# __init__.py

from dagster import (
    Definitions,
    ScheduleDefinition,
)
...

save_model_schedule = ScheduleDefinition(
    job=create_spam_model_job,
    cron_schedule="0 0 1 * *",  # every month
)

defs = Definitions(
    ...
    schedules=[create_spam_model_schedule],
    jobs=[create_spam_model_job],
)
```



# Adding user messages to our spam filter

We want our spam filter to get better over time, which means that every time the model is rebuilt, it should include new user messages in the training data. We already created a “messages” table in the database, now it’s time to create two new assets to make use of those messages.

To generate sample messages, copy sample_data.py from GitHub. It’s a simple script that creates messages containing either “spammy” terms like “winner”, “urgent”, and “free”, or innocuous terms such as “schedule” and “meeting”.

We’ll now create a new asset to fetch all user messages from the database.


```python
# assets.py

from dagster import asset
from .resources import Database
import pandas as pd
...

@asset
def user_messages(database: Database) -> pd.DataFrame:
    results = database.query(
        f"""
        SELECT
            spam,
            body
        FROM
            messages
    """
    )
    results.rename(columns={"body": "text"}, inplace=True)
    return results
```


We were previously passing the output of the sms_spam_examples() asset directly into the spam_model() asset. We’re going to create an intermediary asset to combine the output of both sms_spam_examples() and user_messages(), which can be passed into spam_model().


```python
import pandas as pd
...

@asset
def spam_dataset(sms_spam_examples, user_messages) -> pd.DataFrame:
    joined_dataframe = pd.concat([sms_spam_examples, user_messages], ignore_index=True)
    return joined_dataframe.sample(frac=1).reset_index(drop=True)
```


This simple asset concatenates the two upstream assets and shuffles their contents.

Now pass the output of spam_dataset() into spam_model().


```python
@multi_asset(
    outs={
        "spam_bag_of_words": AssetOut(),
        "non_spam_bag_of_words": AssetOut(),
        "spam_training_dataframe": AssetOut(),
        "spam_testing_dataframe": AssetOut(),
    }
)
def spam_model(spam_dataset: pd.DataFrame):
    def preprocess_text(text: str) -> str:
        return text.lower().translate(str.maketrans("", "", string.punctuation))

    def calculate_word_frequencies(words: list, shared_words: set) -> dict:
        word_counts = Counter(words)
        total_words = len(words)
        return {word: word_counts[word] / total_words for word in shared_words}

    spam_dataset["text"] = spam_dataset["text"].apply(preprocess_text)
```

# Exposing our model with an API

We previously installed Flask, which we’ll use to create a simple spam endpoint.


```python
# api.py

from flask import Flask, jsonify, request
from spam_filter import spam

app = Flask(__name__)

@app.route("/api/find_spam", methods=["POST"])
def find_spam():
    payload = request.get_json()
    text_value = payload.get("text", None)

    if text_value is None:
        return jsonify({"success": False, "error": "Missing 'text' parameter"})

    try:
        is_spam = spam.is_spam(text_value)
        return jsonify({"success": True, "is_spam": is_spam})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

if __name__ == "__main__":
    app.run(port=5000)
```


Note that our API uses the spam.py file from before, but we’ll need to add the is_spam() function. This function queries the database to find the path to the currently active model and reads the file before passing the contents into compare_text_to_bow().


```python
import duckdb
import json

def is_spam(text):
    with duckdb.connect("./database.duckdb") as conn:
        df = conn.execute("SELECT * FROM models WHERE status = 'active'").fetchdf()
        path_value = df.loc[0, "path"]
        with open(f"./weights/{path_value}", "r") as f:
            data = json.load(f)
            spam_bow = data.get("spam_bow")
            non_spam_bow = data.get("non_spam_bow")
            percent_spam = data.get("percent_spam")

    return compare_text_to_bow(text.split(), spam_bow, non_spam_bow, percent_spam)
```



# Testing our spam filter

To test the results of our spam filter, run the API script in the terminal.


```python
$ python api.py
* Serving Flask app 'api'
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on <http://127.0.0.1:5000>
Press CTRL+C to quit
127.0.0.1 - - [06/Sep/2023 03:15:13] "POST /api/find_spam HTTP/1.1" 200 -
```

Next use cURL in another terminal window to try different messages to see if they get flagged as spam.

```python
curl -X POST -H "Content-Type: application/json" -d '{"text": "you are the winner!"}' <http://127.0.0.1:5000/api/find_spam>

{"is_spam":true,"success":true}
```

# **Moving the pipeline into production**

The core _Bag of Words_ algorithm, though simple, is quite powerful and can be effective in production. But with any sample project, it’s a good idea to consider what would need to change in order to move this into production. We’ll touch on a few pieces here, but for a more in depth guide, take a look at Dagster’s guide on[ transitioning data pipelines from development to production](https://docs.dagster.io/guides/dagster/transitioning-data-pipelines-from-development-to-production).


## **Swapping local resources for production resources**

We’re using local file storage to store models and DuckDB as an application database. In production, you can swap out local file storage for a service like Amazon S3 buckets. To reduce latency, the newest version of the model can be deployed in a Docker container along with your application.

When accessing production message data, you’ll probably use a database like Postgres or MySQL. Consider a read replica of relevant tables if needed when accessing large volumes of data from your Dagster jobs.


## I/O Manager

Like the file storage and application database, you may need an alternative to the in-memory DuckDB I/O Manager in production. The post “[Poor Man’s Data Lake with MotherDuck](https://dagster.io/blog/poor-mans-datalake-motherduck#step-2-replacing-the-iomanager)” walks through porting a local project to[ MotherDuck](https://motherduck.com/), which allows a one-line change from local DuckDB development to a serverless DuckDB platform.

You can also use tools like Snowflake or Databricks.


## Sampling message data

Our model uses all existing user messages to build each month’s new model. This won’t work for a large-scale application. An alternative is to sample results and grab only a subset of relevant messages to build the model.


## Testing and logging

We didn’t dig into testing or logging in this tutorial, but these are essential parts of a robust production pipeline. Take a look at Dagster’s guides on[ effective testing](https://docs.dagster.io/concepts/testing#testing) and[ logging](https://docs.dagster.io/concepts/logging/python-logging#python-logging).


# Wrapping up

Improving data quality is an ongoing battle in any data team.  Spam erodes data quality and data trust. When we ingest user-provided data, should always consider the challenge of abuse and spam.  Hopefully our tutorial will help pave the way for better data quality in your organization, as this approach can be applied to many more use cases than just messaging.
