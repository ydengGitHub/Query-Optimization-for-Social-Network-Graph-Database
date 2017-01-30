# Query-Optimization-for-Social-Network-Graph-Database
Graph databases are widely used for big data problems and scenarios which are really hard to be presented using relational database. How to represent and be able to query time-varying graph database has been the major challenge in implementing graph databases. In this project, we designed, implemented and tested an improved time-varying data structure for neo4j graph database based on previous models. In particular, we proposed new time tree model for the database. Extra edges were created to achieve better querying performance among data nodes and time tree node. The actual data we used to demonstrate our time-varying graph database is tweets that contain movies with positive attitude in their text. User nodes and Movie nodes were also added depend on different models. We demonstrated that query performance of database can be improved dramatically in the new model even both using time tree.

PROPOSED WORK 
1	REST/Search API
The Twitter REST APIs provide programmatic access to read and write Twitter data. Twitter4j for Twitter API 1.1 was used in this study [5]. Twitter4j is a Java library that gives access to the Twitter REST APIs. We have integrated Twitter4j in our Java application. Our tweet colleting algorithm uses movie hash tag as search parameter. Twitter4j is also capable of filtering tweet locations using latitude, longitude and radius as parameters. Since location is not relevant to this study, so we did not collect location data.
2	Tweet collection
Once a tweet is collected, we create the tweet node using a Tweet class, it contains information of the tweet, including: user ID, user Name, tweet content as string, create time, hashtag. Depend on database model, we might also need to construct user node or movie node.

3	Database construction
In this study, we compared two models for constructing our database (section 4). For each database, we will need to pre-generate time nodes when initiating the graph database. The time tree consists several levels: year, month, week, day, and hour. (Details will be discussed in section 4.)
After time tree is constructed, tweet nodes are generated using tweets from twitter4j queries. User or movie nodes are also needed according to different data model.

4	Query design
In order to test performance of our graph database models, we have designed several queries:
1.	Most tweeted k movies during a given time period.
2.	Least tweeted k movies during a given time period.
3.	Number of tweets of a specific movie during a given time period.
4.	Which day did people tweet most about a specific movie.
5.	Which week did people tweet  most about a specific movie.

5	Performance Analysis
All queries in 3.4 were ran multiple times using graph databases built using two different graph database models. For each model, we have constructed graph database with variable sizes. This way we demonstrate performance is consistent among databases with different sizes.

Graph Database Design
In this project, we designed two Neo4j Graph Database Models to store the collected Twitter data. The first model is designed according to the paper published by Cattuto et al., and then we tried to make several improvements over the Model 1 to get the Model 2.

Model 1
The design of Model 1 follows exactly the database design in Cattuto et al.’s paper paper (as shown in Figure 2 Database Model 1).  
•	A time dependent graph is accessed as a RUN node, and is connected with the REF (reference) node through a HAS_RUN relation.
•	RUN nodes have RUN_TIMELINE relations to all the Year nodes (each Year node represent a physical year, and currently we just have 1 year’s data in our database, so we just have 1 Year node).
•	Then we use the Year node as root to build a tree to represent the temporal hierarchy of the dataset.
•	Nodes at each level of the tree represent different time units and labeled accordingly, Month, Week, Day, and Hour.
•	Nodes at each level of the tree have NEXT_LEVEL relations to the nodes at the level below, for example, each Year node has NEXT_LEVEL relations to 12 Month nodes representing 12 months of the year.
•	At the bottom of the tree, each Hour node has a NEXT_HOUR relation to the successive Hour node. 
•	Each Hour nodes have HOUR_TWEET relations to all the Twitter Node whose tweets posted within that hour.
•	Each Twitter node has the Movie name as its property and label, as well as posted time and tweet content as its properties, and has a User_Tweet relation with a User node who posted the tweet.
•	The User node contains the user ID and name as its properties and is connected with RUN node through the RUN_USER relation.


Model 2
To improve the query performance, we made several modifications over the Model 1 to get the Model 2 (Figure 3 Database Model 2):
•	Remove the User nodes and store the user information in the Twitter node as its property.
•	Add Movie Nodes to represent each movie. And each movie node contains the movie name as its property.
•	Each Movie Node is connected with each TIMELINE Node, such that there exist tweet related to the movie within the time, with a MOVIE_YEAR/MOVIE_MONTH/MOVIE_WEEK/MOVIE_DAY/MOVIE_HOUR relation. Each one of these relations has a property NumberOfTweets to store the number of tweets posted related the Movie within the time period. And this value can be calculated automatically when we collect or add data to the database. For example, the MOVIE_MONTH relation between Movie Node (Frozen) and the second Month Node has a property NumberOfTweets whose value represent how many tweets posted within the February 2015 contains the #Frozen hashtag. And in model 1, you have to traverse all the Hour node related to the month and count the number of Twitter nodes with “Frozen” label of each Hour node to get this value.
•	Beside the NEXT_HOUR relation between adjacent Hour Nodes, we also add NEXT_DAY, NEXT_WEEK, NEXT_MONTH relations to connect adjacent DAY, WEEK, MONTH nodes. So we don’t need to always go to the bottom of the Time Line tree to traverse the database.
