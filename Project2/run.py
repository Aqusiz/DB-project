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

def cosine_similarity(v1, v2):
    dot_product = 0
    v1_size = 0
    v2_size = 0
    for i in range(v1):
        dot_product += v1[i] * v2[i]
        v1_size += v1[i] ** 2
        v2_size += v2[i] ** 2
    return dot_product / (v1_size * v2_size) ** 0.5

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
        # Process CSV file
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
        # Insert data
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
        try:
            cursor.execute('DELETE FROM movie WHERE id = %s', (movie_id,))
            connection.commit()
        except:
            print(f'Movie {movie_id} does not exist')
            return

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
        try:
            cursor.execute('DELETE FROM booking WHERE audience_id = %s', (audience_id,))
            cursor.execute('DELETE FROM audience WHERE id = %s', (audience_id,))
            connection.commit()
        except:
            print(f'Audience {audience_id} does not exist')
            return

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
    movie_id = int(input('Movie ID: '))

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM movie WHERE id = %s', (movie_id,))
        movie = cursor.fetchall()
        # error message
        if len(movie) == 0:
            print(f'Movie {movie_id} does not exist')
            return
        cursor.execute('SELECT id, name, gender, age, rating'
                        ' FROM audience a JOIN booking b ON a.id = b.audience_id'
                        ' WHERE b.movie_id = %s', (movie_id,))
        audiences = cursor.fetchall()
        print("-" * 80)
        print(f"{'ID':<5}{'Name':<20}{'Gender':<10}{'Age':<10}{'Rating':<10}")
        print("-" * 80)
        for audience in audiences:
            Rating = audience['rating'] if audience['rating'] is not None else 'None'
            print(f"{audience['id']:<5}{audience['name']:<20}{audience['gender']:<10}{audience['age']:<10}{Rating:<10}")
        print("-" * 80)

    # YOUR CODE GOES HERE
    pass


# Problem 11 (5 pt.)
def print_movies_for_audience():
    # YOUR CODE GOES HERE
    audience_id = int(input('Audience ID: '))

    with connection.cursor(dictionary=True) as cursor:
        # error message
        cursor.execute('SELECT * FROM audience WHERE id = %s', (audience_id,))
        audience = cursor.fetchall()
        if len(audience) == 0:
            print(f'Audience {audience_id} does not exist')
            return
        cursor.execute('SELECT id, title, director, price, rating'
                        ' FROM movie m JOIN booking b ON m.id = b.movie_id'
                        ' WHERE b.audience_id = %s', (audience_id,))
        movies = cursor.fetchall()
        print("-" * 110)
        print(f"{'ID':<5}{'Title':<50}{'Director':<30}{'Price':<10}{'Rating':<10}")
        print("-" * 110)
        for movie in movies:
            Rating = movie['rating'] if movie['rating'] is not None else 'None'
            print(f"{movie['id']:<5}{movie['title']:<50}{movie['director']:<30}{movie['price']:<10}{Rating:<10}")
        print("-" * 110)

    # YOUR CODE GOES HERE
    pass


# Problem 12 (10 pt.)
def recommend():
    # YOUR CODE GOES HERE
    audience_id = int(input('Audience ID: '))
    user_item_matrix = [[]]
    user_similarity_vector = []

    with connection.cursor(dictionary=True) as cursor:
        # error message
        cursor.execute('SELECT * FROM audience WHERE id = %s', (audience_id,))
        audience = cursor.fetchall()
        if len(audience) == 0:
            print(f'Audience {audience_id} does not exist')
            return
        # init matrix by 0
        cursor.execute('SELECT COUNT(*) AS cnt FROM movie')
        movie_cnt = cursor.fetchall()[0]['cnt']
        cursor.execute('SELECT COUNT(*) AS cnt FROM audience')
        audience_cnt = cursor.fetchall()[0]['cnt']

        user_item_matrix = [[0 for _ in range(movie_cnt + 1)] for _ in range(audience_cnt + 1)]
        user_similarity_vector = [0 for _ in range(audience_cnt + 1)]
        # fill user_item_matrix
        cursor.execute('SELECT audience_id, movie_id, rating FROM booking')
        bookings = cursor.fetchall()
        
        for booking in bookings:
            user_item_matrix[booking['audience_id']][booking['movie_id']] = booking['rating']
            user_item_matrix[booking['audience_id']][0] += 1 # count of rated movies
        # error if the user has not rated any movie
        if user_item_matrix[audience_id][0] == 0:
            print('Rating does not exist')
            return
        # fill the rest of user_item_matrix by average
        for i in range(1, audience_cnt + 1):
            if i == audience_id:
                continue
            if user_item_matrix[i][0] == 0:
                continue

            rate_sum = sum(user_item_matrix[i][1:])
            for j in range(1, movie_cnt + 1):
                if user_item_matrix[i][j] == 0:
                    user_item_matrix[i][j] = rate_sum / user_item_matrix[i][0]
        # fill user_similarity_vector
        for i in range(1, audience_cnt + 1):
            if i == audience_id:
                user_similarity_vector[i] = 1
                continue

            user_similarity_vector[i] = cosine_similarity(user_item_matrix[audience_id][1:], user_item_matrix[i][1:])
        # fill user_item_matrix of audience_id by weighted average
        for i in range(1, movie_cnt + 1):
            if user_item_matrix[audience_id][i] != 0:
                continue

            weighted_sum = 0
            weight_sum = 0
            for j in range(1, audience_cnt + 1):
                if user_item_matrix[j][i] == 0:
                    continue
                weighted_sum += user_item_matrix[j][i] * user_similarity_vector[j]
                weight_sum += user_similarity_vector[j]
            user_item_matrix[audience_id][i] = weighted_sum / weight_sum
        # find movie id with max rating
        max_rated_movie_id = user_item_matrix[audience_id][1:].index(max(user_item_matrix[audience_id][1:]))
        cursor.execute('SELECT id, title, director, price, rating'
                        ' FROM movie LEFT JOIN (SELECT movie_id, AVG(rating) AS rating'
                                                ' FROM booking GROUP BY movie_id) AS avg_rating'
                                                ' ON movie.id = avg_rating.movie_id'
                        ' WHERE id = %s', (max_rated_movie_id,))
        result = cursor.fetchall()[0]
        # print result
        print("-" * 110)
        print(f"{'ID':<5}{'Title':<50}{'Director':<30}{'Avg. Rating':<10}{'Expected rating':<15}")
        print("-" * 110)
        print(f"{result['id']:<5}{result['title']:<50}{result['director']:<30}{result['rating']:<10}{user_item_matrix[audience_id][max_rated_movie_id]:<15}")
        print("-" * 110)

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
            ans = input('Are you sure? (y/n) ')
            if ans == 'y':
                reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()