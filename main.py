import calibro
import argparse


def main():
    parser = argparse.ArgumentParser(description="Index and search Calibre servers.")
    parser.add_argument('-d', '--db', help='Database file. (default: calibro.db', default='calibro.db')
    parser.add_argument('-s', '--search', help='Search query')
    parser.add_argument('-m', '--metadata', help='Metadata fields to search (default: all)', default='all')
    parser.add_argument('-l', '--links_only', action='store_true', help='Return only download URLS for search results', default=False)
    parser.add_argument('-i', '--input', help='Calibre server to Index. Default: None', default=None)
    parser.add_argument('-ls', '--list_servers', action='store_true', help='List servers in DB.', default=False)
    parser.add_argument('-dls','--download_server', help='Download all books from server')
    parser.add_argument('-o', '--download_location', help='Save location')
    args = parser.parse_args()

    db = calibro.Database(args.db)
    if args.input:
        calibro.process_server(calibro.CalibreServer(args.input), db, 1000)
    if args.download_server:
        calibro.download_server(db, args.download_server, args.download_location)
    else:
        if args.list_servers:
            calibro.list_libraries(db)
        else:
            if ',' in args.search and ',' in args.metadata:
                calibro.search(db, list(args.search.split(",")), list(args.metadata.split(",")), args.links_only)
            else:
                print(type(args.search), type(args.metadata))
                calibro.search(db, args.search, args.metadata, args.links_only)


if __name__ == "__main__":
    main()
