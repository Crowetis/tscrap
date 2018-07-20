from query import TwitterSearch
from tweet import TwitterSlicer
from query import TwitterSearchImpl
import sys
import datetime
import logging as log

if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    print("Para salir presione cualquier otra tecla. ")
    print()
    while True:
    
       z = raw_input("Busqueda normal o paralela [n]/[p] : ").lower()
       if z != 'n' and z != 'p':
          print("\nHasta luego...\n")
          sys.exit()
       if z == 'p':
          a = raw_input("Introduzca Primer Parametro : ")
          b = raw_input("Introduzca Segundo Parametro : ")
          search_query = a + " OR " + b
                        
       if z == 'n':
          search_query = raw_input("Ingrese parametro de busqueda : ")
       if not z:
           sys.exit()
       rate_delay_seconds = 0
       error_delay_seconds = 5

       # Example of using TwitterSearch
       twit = TwitterSearchImpl(rate_delay_seconds, error_delay_seconds, None)
       twit.search(search_query)

       # Example of using TwitterSlice
       select_tweets_since = datetime.datetime.strptime("2018-01-01", '%Y-%m-%d')
       select_tweets_until = datetime.datetime.strptime("2016-04-16", '%Y-%m-%d')
       threads = 40

       twitSlice = TwitterSlicer(rate_delay_seconds, error_delay_seconds, select_tweets_since, select_tweets_until,
                              threads)
       twitSlice.search(search_query)

       print("TwitterSearch collected %i" % twit.counter)
       print("TwitterSlicer collected %i" % twitSlice.counter)


