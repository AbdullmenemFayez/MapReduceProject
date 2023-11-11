# MapReduceTask


1. ratings ( UserID, MovieID, Rating ) // where rating represents the rating between
(from 1 to 5) given by the user to the corresponding movieID
2. users ( UserID, Gender, Age)
3. movies ( MovieID, Title, Genres ) // where genres is the classification of the
movie such as comedy, children, action, …. 
Suppose you have been given a task to find the average rating for each movie in 
the form (movieID, Title, avg_rating). Computing the average rating must consider 
the following: 
4. only children and comedy movies
5. consider rating values that are above 2
6. consider ratings from users who’s age is above 25
