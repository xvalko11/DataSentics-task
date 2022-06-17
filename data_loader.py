from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import re
import os
import requests
import zipfile
from sqlalchemy import func

db = SQLAlchemy()


# SQLAlchemy database model for Book
class Book(db.Model):
    book_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String, nullable=False, index=True)
    isbn = db.Column(db.String, nullable=False)
    author = db.Column(db.String, nullable=False, index=True)
    year = db.Column(db.Integer, nullable=False)
    publisher = db.Column(db.String, nullable=False)
    img = db.Column(db.String, nullable=False)

    def __init__(self, isbn, title, author, year, publisher, img):
        self.book_id = None
        self.title = title
        self.isbn = isbn
        self.author = author
        self.year = year
        self.publisher = publisher
        self.img = img


# SQLAlchemy database model for BookReview
class BookReview(db.Model):
    review_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    isbn = db.Column(db.String, nullable=False, index=True)
    rating = db.Column(db.Integer, nullable=False)

    def __init__(self, user_id, isbn, rating):
        self.user_id = user_id
        self.isbn = isbn
        self.rating = rating


# Object for downloading, preparing, loading and getting data to/from db
class DataLoader():
    books = []
    ratings = []

    # Method downloads raw csv data from url and unzips them
    def download_and_unzip_csv(self, url='http://www2.informatik.uni-freiburg.de/~cziegler/BX/BX-CSV-Dump.zip'):
        dir = "data"
        filename = "csv-dump.zip"
        filepath = dir+"/"+filename
        if not os.path.exists(dir):
            os.makedirs(dir)
        with requests.get(url) as request:
            with open(filepath, "wb") as output:
                output.write(request.content)
        with zipfile.ZipFile(filepath) as zip:
            zip.extractall(dir)

    # Method returns average rating and number of reviews of specified book by ISBN
    def get_avg_rating_of_book_by_isbn(self, isbn):
        reviews = BookReview.query.filter(
            BookReview.isbn == isbn.upper()).all()
        avg = db.session.query(func.avg(BookReview.rating).label(
            'average')).filter(BookReview.isbn == isbn.upper()).first()
        try:
            avg = format(avg[0], ".2f")
        except TypeError:
            avg = str(-1)
        return avg, len(reviews)

    # Method returns Book object specified by ID
    def get_book_by_id(self, book_id):
        return Book.query.get(book_id)

    # Method returns multiple Book objects specified by list of ISBNs
    def get_books_by_isbn(self, isbns):
        books = []
        for i in range(len(isbns)):
            book = Book.query.filter(
                Book.isbn == isbns[i].upper()).first()
            if(book is None):
                book = Book.query.filter(
                    Book.isbn.contains(isbns[i][1:])).first()
            books.append(book)

        return books

    # Method returns multiple Book objects if their title contains param title
    def search_for_books(self, title):
        books = Book.query.filter(Book.title.contains(title))
        return books

    # Method loads BookRatings from csv to list of objects and filter the ones with 0 rating
    def load_all_book_ratings_from_csv(self, filepath):
        with open(filepath, 'r') as csv_file:
            ratings = pd.read_csv(csv_file, encoding='cp1251',
                                  sep=';', error_bad_lines=False)
            ratings = ratings[ratings['Book-Rating'] != 0]
            for row in ratings.iterrows():
                user_id = row[1][0]
                isbn = row[1][1]
                rating = row[1][2]
                book_review = BookReview(user_id, isbn, rating)
                self.ratings.append(book_review)

    # Method loads Books from csv to list of objects and filter the ones with invalid attributes
    def load_all_books_from_csv(self, filepath):
        with open(filepath, 'r') as csv_file:
            books = pd.read_csv(csv_file, encoding='cp1251',
                                sep=';', error_bad_lines=False)
            books = books.drop_duplicates(subset='Book-Title', keep='first')
            for row in books.iterrows():
                year = row[1][3]
                if(str(year).isdigit()):
                    isbn = str(row[1][0])
                    title = str(row[1][1])
                    author = str(row[1][2])
                    year = int(row[1][3])
                    publisher = str(row[1][4])
                    img = str(row[1][7])

                    if re.match('^\d*[\d|X]$', isbn) is not None:  # basic isbn regex
                        book_review = Book(
                            isbn, title, author, year, publisher, img)
                        self.books.append(book_review)

    # Method initiates and creates database from lists of Books and BookReviews
    def init_db_from_csv(self):
        self.load_all_book_ratings_from_csv('./data/BX-Book-Ratings.csv')
        db.session.add_all(self.ratings)
        self.ratings.clear()
        self.load_all_books_from_csv('./data/BX-Books.csv')
        db.session.add_all(self.books)
        self.books.clear()
        db.session.commit()
