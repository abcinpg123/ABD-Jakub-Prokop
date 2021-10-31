
import pickle
import numpy as np

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if isinstance(category_id, int):
        df = pd.read_sql("""select title, language, category.name from category 
                         LEFT OUTER JOIN film_category on category.category_id = film_category.category_id 
                         INNER JOIN film on film_category.film_id=film.film_id 
                         INNER JOIN language on film.language_id = language.language_id 
                         WHERE category.category_id = (%s)
                         ORDER BY title, language
                         """, params=[category_id], con=connection)
        return df
    return None


def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id, int):
        df = pd.read_sql("""select category.name, count(film.film_id) from category 
                         LEFT OUTER JOIN film_category on category.category_id = film_category.category_id 
                         INNER JOIN film on film_category.film_id=film.film_id 
                         WHERE category.category_id = (%s)
                         GROUP BY category.name
                         """, params=[category_id], con=connection)
        return df
    return None


def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if (isinstance(min_length, int) or isinstance(min_length, float)) \
            and (isinstance(max_length, int) or isinstance(max_length, float)):
        df = pd.read_sql("""select length, count(film.film_id) from  film 
                         WHERE length > (%s) and length < (%s)
                         GROUP BY length
                         """, params=[min_length, max_length], con=connection)
        return df
    return None


def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(city, str):
        df = pd.read_sql("""select city, first_name, last_name from customer 
        INNER JOIN address on customer.address_id=address.address_id 
        INNER JOIN city on address.city_id = city.city_id
                         WHERE city.city = (%s)
                         """, params=[city], con=connection)
        return df
    return None


def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if (isinstance(length, int) or isinstance(length, float)):
        df = pd.read_sql("""select length, AVG(amount) from film 
        LEFT OUTER JOIN inventory ON film.film_id = inventory.film_id 
        INNER JOIN rental ON inventory.inventory_id = rental.inventory_id 
        INNER JOIN payment ON rental.rental_id = payment.rental_id
                         WHERE length = (%s)
                         GROUP BY length
                         """, params=[length], con=connection)
        return df
    return None


def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if (isinstance(sum_min, int) or isinstance(sum_min, float)):
        df = pd.read_sql("""select first_name, last_name, sum(length) from customer 
        INNER JOIN rental on rental.customer_id=customer.customer_id 
        INNER JOIN inventory on rental.inventory_id = inventory.inventory_id 
        INNER JOIN film on inventory.film_id = film.film_id 
        GROUP BY customer.first_name, last_name 
        having  sum(length) > (%s)
                         """, params=[sum_min], con=connection)
        return df
    return None


def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(name, str):
        df = pd.read_sql("""SELECT name, AVG(length), SUM(length), MIN(length), MAX(length) from category 
        INNER JOIN film_category on film_category.category_id = category.category_id 
        INNER JOIN film on film_category.film_id = film.film_id GROUP BY name
            having  category.name = (%s)""", params=[name], con=connection)
        return df
    return None


