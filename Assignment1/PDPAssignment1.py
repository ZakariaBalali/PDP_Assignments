# Code to execute: python PDPAssignment1.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data

# Libraries needed to execute
from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRatingCounter(MRJob):

    # This function defines the steps that are going to be executed and in which order they will be executed
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_counted_movies)
        ]

    # This function loads the data by defining the columns and line splitting the lines
    # We only need the movieID in this case, so we can return that
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # combine the count of ratings with their movie ids
    def reducer_count_ratings(self, movieID, rating):
        yield None, (sum(rating), movieID)

    # This function sorts the movies based on their rating
    # It also reverses it so that the movies are ordered by most ratings to the least amount of ratings.
    def reducer_sort_counted_movies(self, _, movieRatingCounts):
        for ratingCount, key in sorted(movieRatingCounts, reverse=True):
            yield (int(key), int(ratingCount))


# This line is needed to make the code run
if __name__ == '__main__':
    MovieRatingCounter.run()
