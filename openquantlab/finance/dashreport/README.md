# Dash Financial Report

This is a demo of the [Dash] financial report app developed in Plotly.


## Getting Started

### Running the app locally

At pub_finance directory, execute the command "sudo nohup /home/ubuntu/miniconda3/bin/python -m finance.dashreport.dash_wsgi > dash.log 2>&1 &"

Debug command "sudo nohup /home/ubuntu/miniconda3/bin/python -u -m finance.dashreport.dash_wsgi 2>&1 | tee dash.log &"

First create a virtual environment with conda or venv inside a temp folder, then activate it.

```
virtualenv venv

# Windows
venv\Scripts\activate
# Or Linux
source venv/bin/activate

```

Clone the git repo, then install the requirements with pip

```

git clone https://github.com/KumailQazi/dash-financial-report1 
cd dash-financial-report
pip install -r requirements.txt

```

Run the app

```

python app.py

```

## About the app

This is an interactive, multi-page report which displays a variety of tables, bullet points, and Plotly interactive plots in a report format. The app incorporates custom local and external CSS to display distinct pages for PDF print.



## Built With

- [Dash](https://dash.plot.ly/) - Main server and interactive components
- [Plotly Python](https://plot.ly/python/) - Used to create the interactive plots

The following are screenshots for the app in this repo:
