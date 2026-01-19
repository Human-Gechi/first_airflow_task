import gzip
import csv

def write_chunk(rows, index):
    #Output paths for chunked files
    output_path = f"/tmp/wikipedia_pageviews_{index}.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out, delimiter='|')
        writer.writerows(rows)
    print(f"Saved {output_path}")

def dataframe_parser():
    """Chunk files into rows of 1million """
    current_chunk_rows = []
    chunk_count = 0
    chunk_size = 1000000
    #Input .gz file
    with gzip.open("/tmp/wikipedia_pageviews.gz", "rt") as f_in:
        for line in f_in:
            parts = line.split()
            if len(parts) < 4:
                continue
            #Splitting data intor respective parts
            try:
                domain = parts[0].replace('""', "").strip()
                title = " ".join(parts[1:-2]).strip()
                views = int(parts[-2])
                bytes_size = int(parts[-1])
                current_chunk_rows.append([domain, title, views, bytes_size])

                #Handling file chunking
                if len(current_chunk_rows) >= chunk_size:
                    write_chunk(current_chunk_rows, chunk_count)
                    current_chunk_rows = []
                    chunk_count += 1

            except (ValueError, IndexError):
                continue
        #Write last set of rows if < 1000000
        if current_chunk_rows:
            write_chunk(current_chunk_rows, chunk_count)