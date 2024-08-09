from dbfread import DBF
import datetime
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
    elif isinstance(value, datetime.date):
        return f"\"{value.strftime('%Y-%m-%d')}\""
    else:
        return f"\"{str(value)}\""

def generate_client_sql(client_data, new_id, table_name='proprietarios'):
    """ Gera o comando SQL para inserção de um cliente. """
    try:
        # Mapeamento dos campos
        field_mapping = {
            'NMCLIENT': 'nome_completo',
            'ENCLIENT': 'endereco',
            'BRCLIENT': 'bairro',
            'CICLIENT': 'cidade',
            'ESCLIENT': 'estado',
            'CECLIENT': 'cep',
            'CGCLIENT': 'cnpj_cpf',
            'IECLIENT': 'insc_est_rg',
            'FNCLIENT': 'celular',
            'OBS1': 'observacoes',
        }

        # Mapeia as colunas que não devem ser incluídas
        ignore_columns = {'FDCLIENT', 'FACLIENT', 'ULTIMO'}

        # Juntar OBS1, OBS2, OBS3 em uma única coluna 'observacoes'
        observacoes = '\n'.join([
            client_data.get('OBS1', ''), 
            client_data.get('OBS2', ''), 
            client_data.get('OBS3', '')
        ]).strip()

        # Atualiza o client_data para refletir o campo combinado
        client_data['OBS1'] = observacoes  # OBS1 é utilizado para 'observacoes'

        # Converte as chaves e valores de acordo com o mapeamento
        columns = [field_mapping.get(col, col) for col in field_mapping.keys() if col in client_data and col not in ignore_columns]
        values = []

        # Debug: Verificar os dados antes de processamento
        for dbf_field, sql_field in field_mapping.items():
            if dbf_field in client_data and dbf_field not in ignore_columns:
                value = client_data.get(dbf_field, '')
                formatted_value = escape_sql_string(value)
                values.append(formatted_value)

        # Remove duplicatas na lista de colunas
        columns = list(dict.fromkeys(columns))  # Remove colunas duplicadas

        sql = f"INSERT INTO {table_name} (id, {', '.join(columns)}) VALUES ({new_id}, {', '.join(values)});\n"
        return sql
    except Exception as e:
        print(f"[DEBUG] Erro ao gerar SQL para cliente ID {new_id}: {e}")
        traceback.print_exc()
        raise

def generate_vehicle_sql(vehicle_data, new_id_map, field_mapping, table_name='veiculos'):
    try:
        valores = []
        colunas = []
        
        for dbf_field, sql_field in field_mapping.items():
            valor = vehicle_data.get(dbf_field, '')
            if isinstance(valor, datetime.date):
                valor_formatado = f"\"{valor.strftime('%Y-%m-%d')}\""
            elif dbf_field in ['DATAENT', 'DATLIB'] and not valor:
                valor_formatado = "NULL"
            else:
                valor_formatado = escape_sql_string(valor)
            valores.append(valor_formatado)
            colunas.append(sql_field)
        
        # Adicionar 'proprietario_cod' apenas uma vez
        codcli = vehicle_data.get('CODCLI', None)
        new_proprietario_id = new_id_map.get(codcli, "NULL")
        valores.append(str(new_proprietario_id))
        colunas.append('proprietario_cod')

        # Concatenar os campos OBS1, OBS2 e OBS3
        obs1 = vehicle_data.get('OBS1', '')
        obs2 = vehicle_data.get('OBS2', '')
        obs3 = vehicle_data.get('OBS3', '')
        obs = escape_sql_string(obs1 + '\n' + obs2 + '\n' + obs3)
        valores.append(obs)
        colunas.append('obs')
        
        # Montar a instrução SQL INSERT INTO
        sql = f"INSERT INTO {table_name} ({', '.join(colunas)}) VALUES ({', '.join(valores)});\n"
        return sql
    except Exception as e:
        print(f"[DEBUG] Erro ao gerar SQL para veículo com CODCLI {codcli}: {e}")
        traceback.print_exc()
        raise
    
def main():
    field_mapping = {
        'PLACA': 'placa',
        'MARCA': 'marca',
        'TIPO': 'tipo',
        'APREEN': 'apr_detran',
        'MODELO': 'modelo',
        'COR': 'cor',
        'RENAVAM': 'renavam',
        'CHASSI': 'chassi',
        'MOTIVO': 'motivo',
        'PERICC': 'pericia_ic',
        'DATENT': 'dt_entrada',
        'DATLIB': 'dt_liberacao',
    }

    start_id = 1

    try:
        encoding = 'MacRoman'

        # Load CLIENTES.DBF
        print("[DEBUG] Carregando CLIENTES.DBF...")
        clientes_table = DBF('CLIENTES.DBF', encoding=encoding)
        print("[DEBUG] Campos disponíveis em CLIENTES.DBF:")
        print(clientes_table.field_names)  # Exibir nomes dos campos disponíveis

        # Mapear o CLICOD para o novo ID
        clientes_map = {}
        current_id = start_id
        sql_clientes = []

        for record in clientes_table:
            clicod = record['CLICOD']
            client_data = {k: v for k, v in record.items() if k != 'CLICOD'}  # Excluir CLICOD dos campos
            clientes_map[clicod] = current_id
            sql = generate_client_sql(client_data, current_id)
            sql_clientes.append(sql)
            current_id += 1

        # Write SQL for proprietarios
        with open('inserir_proprietarios.sql', 'w', encoding='utf-8') as sql_file:
            print(f"[DEBUG] Gravando {len(sql_clientes)} registros de clientes no arquivo inserir_proprietarios.sql...")
            sql_file.writelines(sql_clientes)

        # Load veiculo.dbf and process it
        print("[DEBUG] Carregando veiculo.dbf...")
        veiculos_table = DBF('veiculo.dbf', encoding=encoding)
        sql_veiculos = []

        for record in veiculos_table:
            sql_veiculos.append(generate_vehicle_sql(record, clientes_map, field_mapping))

        # Write SQL for veiculos
        with open('inserir_veiculos.sql', 'w', encoding='utf-8') as sql_file:
            print(f"[DEBUG] Gravando {len(sql_veiculos)} registros de veículos no arquivo inserir_veiculos.sql...")
            sql_file.writelines(sql_veiculos)

        print("Scripts SQL gerados com sucesso.")
    except Exception as e:
        print(f"Erro ao processar os arquivos DBF: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
