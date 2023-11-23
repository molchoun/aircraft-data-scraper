import pandas as pd
import os


def csv_to_xlsx(filename):
    try:
        df = pd.read_csv(filename)
        file_directory = os.path.dirname(os.path.abspath(filename))
        file_name, _ = os.path.splitext(os.path.basename(filename))
        xlsx_filename = os.path.join(file_directory, f'{file_name}.xlsx')
        df.to_excel(xlsx_filename, index=False)

        print(f"Conversion successful. Excel file saved at: {xlsx_filename}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    csv_to_xlsx("data/ARMASTER11-21-23.csv")
