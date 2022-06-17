import pandas as pd
import numpy as np


class Model:
    def __init__(self):
        self.dataset = pd.DataFrame()

    def load_sql(self, db):
        # load books
        books = pd.read_sql(
            'SELECT * FROM book', db.session.bind)
        # load ratings
        ratings = pd.read_sql(
            'SELECT * FROM book_review', db.session.bind)
        self.dataset = pd.merge(ratings, books, on=['isbn'])
        self.dataset = self.dataset.apply(
            lambda x: x.str.lower() if(x.dtype == 'object') else x)

    def load_csv(self):
        # load ratings
        ratings = pd.read_csv('./BX-Book-Ratings.csv',
                              encoding='cp1251', sep=';')
        ratings = ratings[ratings['rating'] != 0]
        # load books
        books = pd.read_csv('./BX-Books.csv', encoding="cp1251",
                            escapechar="\\", quotechar="\"", sep=";")
        # users_ratigs = pd.merge(ratings, users, on=['user_id'])
        self.dataset = pd.merge(ratings, books, on=['isbn'])
        self.dataset = self.dataset.apply(
            lambda x: x.str.lower() if(x.dtype == 'object') else x)

    def get_reviews(self, isbn):
        author = self.dataset["author"][self.dataset["isbn"]
                                        == isbn][:1].squeeze()
        if(len(author) == 0):
            return []
        reviewers = self.dataset['user_id'][(self.dataset['isbn'] == isbn) & (
            self.dataset['author'].str.contains(author))]
        reviewers = reviewers.tolist()
        reviewers = np.unique(reviewers)
        # final dataset
        return self.dataset[(
            self.dataset['user_id'].isin(reviewers))]

    def get_books(self, isbn, threshold):
        reviews = self.get_reviews(isbn)
        if(len(reviews) == 0):
            return []
        # Number of ratings per other books in dataset
        nof_rating_per_book = reviews.groupby(
            ['isbn']).agg('count').reset_index()
        # select only books which have actually higher number of ratings than threshold
        books_to_compare = nof_rating_per_book[
            'isbn'][nof_rating_per_book['user_id'] >= threshold].unique()
        books_to_compare = books_to_compare.tolist()

        return reviews[[
            'user_id', 'rating',  'isbn']][reviews['isbn'].isin(books_to_compare)]

    def get_recommendations(self, isbn, max_books):

        ratings_data_raw = self.get_books(isbn, 8)

        if(len(ratings_data_raw) == 0):
            return []

        # group by User and Book and compute mean
        ratings_data_raw_nodup = ratings_data_raw.groupby(
            ['user_id', 'isbn'])['rating'].mean()

        # reset index to see user_id in every row
        ratings_data_raw_nodup = ratings_data_raw_nodup.to_frame().reset_index()

        dataset_for_corr = ratings_data_raw_nodup.pivot(
            index='user_id', columns='isbn', values='rating')

        result_list = []
        worst_list = []

        # Take out the selected book from correlation dataframe
        dataset_of_other_books = dataset_for_corr.copy(deep=False)
        dataset_of_other_books.drop([isbn], axis=1, inplace=True)

        # empty lists
        book_titles = []
        correlations = []
        avgrating = []
        # corr computation
        for book_title in list(dataset_of_other_books.columns.values):
            book_titles.append(book_title)
            correlations.append(dataset_for_corr[isbn].corr(
                dataset_of_other_books[book_title]))
            tab = (ratings_data_raw[ratings_data_raw['isbn'] ==
                                    book_title].groupby(ratings_data_raw['isbn']).mean())
            avgrating.append(tab['rating'].min())

        # final dataframe of all correlation of each book
        corr_fellowship = pd.DataFrame(list(zip(
            book_titles, correlations, avgrating)), columns=['book', 'corr', 'avg_rating'])
        corr_fellowship.head()

        # top n books with highest corr
        result_list = corr_fellowship.sort_values(
            'corr', ascending=False).head(max_books).values.tolist()

        # worst n books
        worst_list.append(corr_fellowship.sort_values(
            'corr', ascending=False).tail(max_books))
        return result_list
