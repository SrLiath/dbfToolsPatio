from dbfread import DBF
import sys
import traceback

def escape_sql_string(value):
    """ Escapa e formata o valor para inserção em SQL. """
    if value is None or value == '':
        return "NULL"
    elif isinstance(value, str):
        # Substitui quebras de linha por \n para o SQL
        return f"\"{value.replace('\"', '\"\"').replace('\n', '\\n')}\""
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return f"\"{str(value)}\""

def generate_tipo_veiculos_sql(record, table_name='tipoVeiculos'):
    """ Gera o comando SQL para inserção de um tipo de veículo. """
    try:
        field_mapping = {
            'TIPO': 'tipo',
            'DIARIA': 'diaria',
            'GUINCHO': 'guincho'
        }

        columns = []
        values = []
        
        for dbf_field, sql_field in field_mapping.items():
            if dbf_field in record:
                value = record[dbf_field]
                formatted_value = escape_sql_string(value)
                columns.append(sql_field)
                values.append(formatted_value)

        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
        return sql
    except Exception as e:
        print(f"[DEBUG] Erro ao gerar SQL para tipo de veículo: {e}")
        traceback.print_exc()
        raise

def main():
    try:
        encoding = 'MacRoman'  # Ajuste a codificação se necessário

        # Load TIPOVEI.DBF
        print("[DEBUG] Carregando TIPOVEI.DBF...")
        tipo_veiculos_table = DBF('TIPOVEI.DBF', encoding=encoding)
        print("[DEBUG] Campos disponíveis em TIPOVEI.DBF:")
        print(tipo_veiculos_table.field_names)  # Exibir nomes dos campos disponíveis

        sql_tipo_veiculos = []

        for record in tipo_veiculos_table:
            sql = generate_tipo_veiculos_sql(record)
            sql_tipo_veiculos.append(sql)

        # Write SQL for tipoVeiculos
        with open('inserir_tipo_veiculos.sql', 'w', encoding='utf-8') as sql_file:
            print(f"[DEBUG] Gravando {len(sql_tipo_veiculos)} registros de tipos de veículos no arquivo inserir_tipo_veiculos.sql...")
            sql_file.writelines(sql_tipo_veiculos)

        print("Script SQL para tipoVeiculos gerado com sucesso.")
    except Exception as e:
        print(f"Erro ao processar o arquivo TIPOVEI.DBF: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
