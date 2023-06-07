from fastapi import FastAPI
import pyodbc

app = FastAPI()

# Kết nối đến cơ sở dữ liệu Azure SQL
server = 'lantvh-azure-sql.database.windows.net'
database = 'lantvh-single-db'
username = 'lantvh@vpi.pvn.vn'
password = ''
driver = '{ODBC Driver 18 for SQL Server}'


@app.get("/get/{table_name}")
async def read_record(table_name: str):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword")
    cursor = conn.cursor()
    # Truy vấn dữ liệu từ bảng
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    data = []
    for result in results:
        row_data = {}
        for i, column in enumerate(cursor.description):
            row_data[column[0]] = result[i]
        data.append(row_data)

    return {"table_name": table_name, "data": data}


@app.post("/create/{table_name}")
async def create_record(table_name: str, data: dict):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword")
    cursor = conn.cursor()

    # Tạo câu truy vấn INSERT dựa trên dữ liệu nhận được
    columns = ', '.join(data.keys())
    values = ', '.join([f"'{value}'" for value in data.values()])
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    # Thực thi câu truy vấn INSERT
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": "Data inserted successfully"}


@app.delete("/delete/{table_name}/{id}")
async def delete_record(table_name: str, id: int):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword")
    cursor = conn.cursor()

    # Tạo câu truy vấn DELETE dựa trên ID của bản ghi
    query = f"DELETE FROM {table_name} WHERE id = {id}"

    # Thực thi câu truy vấn DELETE
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"Record deleted successfully"}


@app.put("/update/{table_name}/{id}")
async def update_record(table_name: str, id: int, updated_data: dict):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword")
    cursor = conn.cursor()

    # Tạo câu truy vấn UPDATE dựa trên dữ liệu nhận được
    set_values = ", ".join([f"{column} = '{value}'" for column, value in updated_data.items()])
    query = f"UPDATE {table_name} SET {set_values} WHERE id = {id}"

    # Thực thi câu truy vấn UPDATE
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": "Record updated successfully"}