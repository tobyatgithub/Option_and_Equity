# Option and Equity

### **General**
This project contains mainly two parts:  
1. Data acquisition  
Most data in financial market is not free. One possible alternative is to write your own code and crawl financial data for further analysis. Yahoo Finance API and Google Finance API are two common choice. Here I tried both and ended up with Yahoo Finance API. Both of them has tiny data accuracy issues, but it turns out that Google API has more restrictions.

2. Modeling  
The challenge for data is most logistic-wise, and the challenge for modeling is more technical. For a long period of time, time-series analysis has been an active reaserching area----simply because it is too common in human's normal life. Financial market, heart rate, health record, travling history, climate change...essentially, this world is time-series based. 

In this project, I don't want to apply traditional time-seris algorithms like average moving window, since they have been used many years and did't provide results satisfying enough. Instead, I used XGBoost and Neural Networks to see how these two most successful families of algorithm perform on this task.

### **Content**
1. Data acquisition:  
A stand-alone piece that can keep crawling data on any independent device.


