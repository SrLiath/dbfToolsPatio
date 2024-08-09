import os
from dbfread import DBF
import traceback

def search_placa_in_dbf(dbf_file, placa_value, encoding='MacRoman'):
    try:
        print(f"[DEBUG] Carregando {dbf_file}...")
        table = DBF(dbf_file, encoding=encoding)
        
        if 'PLACA' not in table.field_names:
            print(f"[DEBUG] Coluna 'PLACA' não encontrada em {dbf_file}.")
            return None

        for record in table:
            if record['PLACA'] == placa_value:
                print(f"[DEBUG] Valor '{placa_value}' encontrado em {dbf_file}.")
                return dbf_file  # Retorna o nome do arquivo

        print(f"[DEBUG] Valor '{placa_value}' não encontrado em {dbf_file}.")
        return None

    except Exception as e:
        print(f"Erro ao processar o arquivo {dbf_file}: {e}")
        traceback.print_exc()
        return None

def main(placa_value, directory='.'):
    found_in_files = []
    supported_extensions = ['.dbf', '.DBF']

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(tuple(supported_extensions)):
                dbf_file = os.path.join(root, file)
                result = search_placa_in_dbf(dbf_file, placa_value)
                if result:
                    found_in_files.append(result)  # Adiciona apenas o nome do arquivo encontrado

    if found_in_files:
        print(f"\nO valor '{placa_value}' foi encontrado nos seguintes arquivos:")
        for file in found_in_files:
            print(file)
    else:
        print(f"\nO valor '{placa_value}' não foi encontrado em nenhum arquivo.")

if __name__ == "__main__":
    placa_value = input("Digite o valor da PLACA que deseja procurar: ").strip()
    directory = input("Digite o diretório onde procurar pelos arquivos DBF (ou deixe em branco para usar o diretório atual): ").strip()
    if not directory:
        directory = '.'
    main(placa_value, directory)
