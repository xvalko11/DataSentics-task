from flask import Flask, render_template, request
from book_rec import Model
from data_loader import DataLoader, db
import os

app = Flask(__name__)
app.debug = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data/database.sqlite3'

db.app = app
db.init_app(app)
model = Model()
dl = DataLoader()


# index page
@ app.route('/')
def index():
    return render_template('index.html')


# page where user can choose from found books
@ app.route('/search', methods=['GET', 'POST'])
def search():
    msg = ''
    if request.method == 'POST' and 'title' in request.form:
        title = request.form['title']
        books = dl.search_for_books(title)
    return render_template('search.html', books=books)


# page with the detail of the book and recommended books
@ app.route('/book/<book_id>', methods=['GET', 'POST'])
def book(book_id):
    recommended_books = []
    book = dl.get_book_by_id(book_id)
    avg, count = dl.get_avg_rating_of_book_by_isbn(book.isbn)
    recommended_books_info = model.get_recommendations(
        book.isbn, 5)
    if(len(recommended_books_info) > 0):
        recommended_books_titles = [title[0]
                                    for title in recommended_books_info]
        recommended_books = dl.get_books_by_isbn(recommended_books_titles)
    return render_template('book.html', book=book, recommended_books=recommended_books, rating=avg, count=count)


if __name__ == "__main__":
    # download data and create db if it doesnt exist
    if not os.path.exists("data/database.sqlite3"):
        dl.download_and_unzip_csv()
        db.create_all()
        dl.init_db_from_csv()

    model.load_sql(db)
    app.run()
