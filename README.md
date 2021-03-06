# Droughtly: A Dashboard for Visualizing, Monitoring and Predicting Drought 

This is a drought monitoring and prediction platform built using Dash - interactive Python framework developed by [Plotly](https://plot.ly/). Our solution seeks to augment efforts to improve water sustainability by providing timely and accurate hydrological information to water managers and regulators. Besides displaying key hydrological information, the solution also leverages machine learning technologies such as Artificial Neural Networks to forecast trends in drought prevalence. Lastly, we also use IBM Watson developed chatbot to facilitate the explanation of key features of the data, as well as important hydrological concepts related to drought, water and climate change. 

Our solution focuses on leveraging the large volumes of water related data, by transforming the data into useful information. Droughtly provides a view to regions water resources, both in terms of surface water and groundwater. The interactive nature of the dashboard allows the various datasets to be explored in conjunctively, which should facilitate a better understanding of the terrestrial water system, and its impacts on the water supply infrastructure, such as dams. In addition, by incorporating pre-emptive forecasting of future trends in drought indicators, we move from mitigation to risk reduction. This powerful feature will allow authorities to better plan ahead, to ensure long-term sustainability. Finally, in an effort to educate stakeholders on the information displayed as well as simple concepts in climate change and drought, a chatbot is implemented in the chatbot. The chatbot allows simple questions to be asked regarding the information displayed as well as the relation to drought and climate change.         

## Key features

- Interactive data visualization
- Display of key information, such as averages, dam levels, and drought severity
- Region selector, to facilitate data discovery
- Short to medium term forecasting of drought indicators
- Use of internationally recognized datasets:
  - CSR GRACE RL06 v2 derived terrestrial water storage
  - ERA5-Land monthly precipitation
  - NCAR 3-month SPEI
- Chatbot for user engagement on data and concepts

![](data/dashboard.png)

## Getting Started

### Running the app locally

First create a virtual environment with conda or venv inside a temp folder, then activate it.

```
virtualenv venv

# Windows
venv\Scripts\activate
# Or Linux
source venv/bin/activate

or on macos
conda create --name myenv
conda activate myenv

```

Clone the git repo, then install the requirements with pip

```

git clone https://github.com/BadisaMosesane/water_sustainability
cd water_sustainability
pip install -r requirements.txt

```

Run the app

```
cd waterLytics
python app.py

```

## Sofware Used
- Plotly Dash
- Pandas
- Numpy
- IBM Watson Assistant
- TWC API

## Issues
* chatbot redirects: will like to do trigger within the same page 
* Predictions integrations
* Enhance UI

## Future developments

The following developments are enviosened as enhance the capabilities of the Droughtly, providing a more comprehensive feature assests for water managers. Some of these development are being actively persuade by the Droughtly team, while others will other will no doubt be possible and useful in the future.

### Parameters and real-time datasets

Besides the data already included on the Droughtly, a number of additional datasets can and should be included to cover a full spectrum of hydrologically related variable. This will ensure that variables related to drought conditions in related hydrological components can be displayed and explored. For example, agricultural drought based on soil moisture changes. In-addition, the replacement of current and future datasets with near-real time versions are also envisioned. This will greatly support the effective sustainable management of water resources. Additional datasets that can be added:

  - Soil moisture
  - Temperature 
  - Run-off
  - Regional water use
  - Ground-based observations or IoT sensor networks
<<<<<<< HEAD
=======
  - Weather Company API for real-time data and forecasts, to improve prediction model
>>>>>>> e697bc4b36417cbab3e8aadf7661a5ff6fe76c3c

### Predictions & analytics

Although currently focused on drought forecasting, the prediction function in the Droughtly can be expanded to include predictions of related variables. The prediction function will also be expanded to include more complex machine learning models that take into account the relation between relevant hydrological features. This is an ongoing research topic for the authors of Droughtly. Useful additional features include, the ability to perform basic analytics on the data, to extract further information. These enhancement will no doubt provide the tools necessary for water managers and relevant to stakeholders to understand their water resources better.    

### Scalability

Currently, for ease of demonstration, Droughtly only includes data pertaining to South Africa. This data is organized according to province, allowing a view into relevant information for a particular province. However, the datasets incorporated in Droughtly are published as global datasets. This will allow us to easily scale up the dashboard to a global or regional context. In-addition, the temporal resolution of the datasets allow for various time intervals (e.g. daily, weekly, or even yearly) information to be incorporated. While the spatial resolution is currently set to provincial scale, the datasets do allow for more local scale information to be incorporated. Adding this upscalibility and downscalibility to Droughtly will provide additional feature exploration. Often local scale investigations are preferred when trying to manage water resources. This functionality is expected to by achieved by incorporating a geospatial search function, that allow users to select locations interactively via a map interface.

### Chatbot

Chatbot enhancements should focus on expanding the number of possible questions and answers. Currently, a only a few responses are programmed based on simple questions that users can ask. By expanding the chatbot, the level of user engagement with the dashboard and data can increase.   

## Contributions

We welcome contributions to this work, to contribute simply do a Pull Request on the [repo](https://github.com/BadisaMosesane/water_sustainability)

## License
This work will be openly published under Apache 2.0

## Authors
- Badisa Mosesane
- Yolanda Kanyama

## Built With

- [Dash](https://dash.plot.ly/) - Main server and interactive components
- [Plotly Python](https://plot.ly/python/) - Used to create the interactive plots
- [IBM Watson Assistant](https://www.ibm.com/cloud/watson-assistant) -  for  developing the edu-chatbot

