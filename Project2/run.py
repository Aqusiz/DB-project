from mysql.connector import connect
import csv

connection = connect(
    host='astronaut.snu.ac.kr',
    port=7000,
    user='DB2017_16140',
    password='DB2017_16140',
    db='DB2017_16140',
    charset='utf8'
)

# Problem 1 (5 pt.)
def reset():
    f = open('data.csv', 'r', encoding='utf-8')
    rdr = csv.reader(f)
    next(rdr)
    with connection.cursor(dictionary=True, buffered=True) as cursor:
        cursor.execute('DROP TABLE IF EXISTS booking')
        cursor.execute('DROP TABLE IF EXISTS movie')
        cursor.execute('DROP TABLE IF EXISTS audience')
        # Create tables
        cursor.execute('CREATE TABLE IF NOT EXISTS movie'
                        '(id INT NOT NULL AUTO_INCREMENT,'
                        'title VARCHAR(100) NOT NULL,'
                        'director VARCHAR(100) NOT NULL,'
                        'price INT NOT NULL,'
                        'PRIMARY KEY(id))')
        cursor.execute('CREATE TABLE IF NOT EXISTS audience'
                        '(id INT NOT NULL AUTO_INCREMENT,'
                        'name VARCHAR(100) NOT NULL,'
                        'gender VARCHAR(6) NOT NULL,'
                        'age INT NOT NULL,'
                        'PRIMARY KEY(id))')
        cursor.execute('CREATE TABLE IF NOT EXISTS booking'
                        '(movie_id INT NOT NULL,'
                        'audience_id INT NOT NULL,'
                        'rating INT,'
                        'PRIMARY KEY(movie_id, audience_id),'
                        'FOREIGN KEY(movie_id) REFERENCES movie(id) ON DELETE CASCADE,'
                        'FOREIGN KEY(audience_id) REFERENCES audience(id))')
        cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
        cursor.execute('TRUNCATE TABLE booking')
        cursor.execute('TRUNCATE TABLE movie')
        cursor.execute('TRUNCATE TABLE audience')
        cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
        # Process CSV file and insert data
        movies = {}
        audiences = {}
        bookings = []
        for row in rdr:
            if row[0] not in movies:
                movies[row[0]] = {
                    'title': row[0],
                    'director': row[1],
                    'price': row[2]
                }
            aud_key = f'{row[3]}_{row[4]}_{row[5]}'
            if aud_key not in audiences:
                audiences[aud_key] = {
                    'name': row[3],
                    'gender': row[4],
                    'age': row[5]
                }
            bookings.append((row[0], row[3], row[4], row[5]))

        for movie in movies.values():
            cursor.execute('INSERT INTO movie (title, director, price) VALUES (%s, %s, %s)',
                            (movie['title'], movie['director'], movie['price']))
        for audience in audiences.values():
            cursor.execute('INSERT INTO audience (name, gender, age) VALUES (%s, %s, %s)',
                            (audience['name'], audience['gender'], audience['age']))
        connection.commit()

        for booking in bookings:
            cursor.execute('SELECT id FROM movie WHERE title = %s', (booking[0],))
            movie_id = cursor.fetchall()[0]['id']
            cursor.execute('SELECT id FROM audience WHERE'
                            ' name = %s AND gender = %s AND age = %s',
                            (booking[1], booking[2], booking[3]))
            audience_id = cursor.fetchall()[0]['id']
            cursor.execute('INSERT INTO booking (movie_id, audience_id) VALUES (%s, %s)',
                            (movie_id, audience_id))
        connection.commit()
        cursor.execute('SELECT COUNT(*) FROM booking')

    f.close()
    print('Initialized database')
    pass

# Problem 2 (3 pt.)
def print_movies():
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT id, title, director, price, bookings, ratings'
                        ' FROM movie LEFT JOIN (SELECT movie_id, COUNT(*) AS bookings, AVG(rating) AS ratings'
                                                ' FROM booking GROUP BY movie_id) AS t'
                                                ' ON movie.id = t.movie_id')
        movies = cursor.fetchall()
        print("-" * 120)
        print(f"{'ID':<5}{'Title':<50}{'Director':<30}{'Price':<10}{'Bookings':<10}{'Ratings':<10}")
        print("-" * 120)
        for movie in movies:
            bookings = (movie['bookings'] if movie['bookings'] else 0)
            rating = (movie['ratings'] if movie['ratings'] else "None")
            print(f"{movie['id']:<5}{movie['title']:<50}{movie['director']:<30}"
                    f"{movie['price']:<10}{bookings:<10}{rating:<10}")
        print("-" * 120)

    pass

# Problem 3 (3 pt.)
def print_audiences():
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM audience')
        audiences = cursor.fetchall()
        print("-" * 50)
        print(f"{'ID':<5}{'Name':<20}{'Gender':<10}{'Age':<10}")
        print("-" * 50)
        for audience in audiences:
            print(f"{audience['id']:<5}{audience['name']:<20}"
                    f"{audience['gender']:<10}{audience['age']:<10}")
        print("-" * 50)

    pass

# Problem 4 (3 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = int(input('Movie price: '))
    
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('INSERT INTO movie (title, director, price) VALUES (%s, %s, %s)',
                        (title, director, price))
        connection.commit()

    # success message
    print('A movie is successfully inserted')
    # YOUR CODE GOES HERE
    pass

# Problem 6 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('DELETE FROM movie WHERE id = %s', (movie_id,))
        connection.commit()

    # error message
    print(f'Movie {movie_id} does not exist')

    # success message
    print('A movie is successfully removed')
    # YOUR CODE GOES HERE
    pass

# Problem 5 (3 pt.)
def insert_audience():
    # YOUR CODE GOES HERE
    name = input('Audience name: ')
    gender = input('Audience gender: ')
    age = int(input('Audience age: '))
    
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('INSERT INTO audience(name, gender, age) VALUES(%s, %s, %s)', (name, gender, age))
        connection.commit()
    
    # success message
    print('An audience is successfully inserted')
    # YOUR CODE GOES HERE
    pass

# Problem 7 (4 pt.)
def remove_audience():
    # YOUR CODE GOES HERE
    audience_id = int(input('Audience ID: '))

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('DELETE FROM booking WHERE audience_id = %s', (audience_id,))
        cursor.execute('DELETE FROM audience WHERE id = %s', (audience_id,))
        connection.commit()

    # error message
    print(f'Audience {audience_id} does not exist')

    # success message
    print('An audience is successfully removed')
    # YOUR CODE GOES HERE
    pass

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = int(input('Movie ID: '))
    audience_id = int(input('Audience ID: '))

    with connection.cursor(dictionary=True) as cursor:
    # error message
        cursor.execute('SELECT * FROM movie WHERE id = %s', (movie_id,))
        movie = cursor.fetchall()
        if len(movie) == 0:
            print(f'Movie {movie_id} does not exist')
            return
        cursor.execute('SELECT * FROM audience WHERE id = %s', (audience_id,))
        audience = cursor.fetchall()
        if len(audience) == 0:
            print(f'Audience {audience_id} does not exist')
            return
        cursor.execute('SELECT * FROM booking WHERE movie_id = %s AND audience_id = %s', (movie_id, audience_id))
        booking = cursor.fetchall()
        if len(booking) != 0:
            print('One audience cannot book the same movie twice')
            return
        cursor.execute('INSERT INTO booking(movie_id, audience_id) VALUES(%s, %s)', (movie_id, audience_id))
        connection.commit()

    # success message
    print('Successfully booked a movie')
    # YOUR CODE GOES HERE
    pass

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')
    rating = input('Ratings (1~5): ')
    if rating not in ['1', '2', '3', '4', '5']:
        print('Wrong value for a rating')
        return
    rating = int(rating)

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM movie WHERE id = %s', (movie_id,))
        movie = cursor.fetchall()
        if len(movie) == 0:
            print(f'Movie {movie_id} does not exist')
            return
        cursor.execute('SELECT * FROM audience WHERE id = %s', (audience_id,))
        audience = cursor.fetchall()
        if len(audience) == 0:
            print(f'Audience {audience_id} does not exist')
            return
        cursor.execute('SELECT * FROM booking WHERE movie_id = %s AND audience_id = %s', (movie_id, audience_id))
        booking = cursor.fetchall()
        if len(booking) == 0:
            print('One audience cannot rate a movie without booking it')
            return
        cursor.execute('UPDATE booking SET rating = %s WHERE movie_id = %s AND audience_id = %s', (rating, movie_id, audience_id))
        connection.commit()

    # success message
    print('Successfully rated a movie')
    # YOUR CODE GOES HERE
    pass

# Problem 10 (5 pt.)
def print_audiences_for_movie():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')

    
    # error message
    print(f'Audience {audience_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 11 (5 pt.)
def print_movies_for_audience():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')


    # error message
    print(f'Audience {audience_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 12 (10 pt.)
def recommend():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'Audience {audience_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass


# Total of 60 pt.
def main():
    # initialize database
    reset()

    while True:
        print('============================================================')
        print('1. print all movies')
        print('2. print all audiences')
        print('3. insert a new movie')
        print('4. remove a movie')
        print('5. insert a new audience')
        print('6. remove an audience')
        print('7. book a movie')
        print('8. rate a movie')
        print('9. print all audiences who booked for a movie')
        print('10. print all movies rated by an audience')
        print('11. recommend a movie for an audience')
        print('12. exit')
        print('13. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            print_movies()
        elif menu == 2:
            print_audiences()
        elif menu == 3:
            insert_movie()
        elif menu == 4:
            remove_movie()
        elif menu == 5:
            insert_audience()
        elif menu == 6:
            remove_audience()
        elif menu == 7:
            book_movie()
        elif menu == 8:
            rate_movie()
        elif menu == 9:
            print_audiences_for_movie()
        elif menu == 10:
            print_movies_for_audience()
        elif menu == 11:
            recommend()
        elif menu == 12:
            connection.close()
            print('Bye!')
            break
        elif menu == 13:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()