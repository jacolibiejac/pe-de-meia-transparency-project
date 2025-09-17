import pandas as pd
import json
import time
from datetime import datetime

# Dados da primeira página já coletados
first_page_data = {
    'headers': ['Detalhar', 'Mês Referência', 'UF', 'Município', 'Beneficiário', 'CPF do Beneficiário', 'Representante Legal', 'Valor Disponibilizado (R$)'],
    'data': [
        ['Detalhar', '01/2025', 'RN', 'SÃO GONÇALO DO AMARANTE', 'AAICHA PAMELA VALENTIM OLEGARIO', '***.675.884-**', 'ADRIANO OLEGARIO BEZERRA', '200,00'],
        ['Detalhar', '01/2025', 'PA', 'SANTARÉM', 'AARAO MATOS DE SOUSA', '***.942.982-**', 'ALCILENE MATOS NOGUEIRA', '200,00'],
        ['Detalhar', '01/2025', 'PR', 'SÃO JOSÉ DOS PINHAIS', 'AARAO OTAVIO ARISTIDES', '***.401.059-**', 'ROSE NILDA DE LIMA', '200,00'],
        ['Detalhar', '01/2025', 'MG', 'CONTAGEM', 'AARON ABBAS CASTILLO LABRADOR', '***.684.312-**', 'JAMDY CAROLINA LABRADOR EL FATAYRI', '200,00'],
        ['Detalhar', '01/2025', 'PR', 'CURITIBA', 'AARON ALEJANDRO ALDANA ESTEVE', '***.309.592-**', 'RINA JOSEFINA ESTEVE', '200,00'],
        ['Detalhar', '01/2025', 'MG', 'SANTANA DE PIRAPAMA', 'AARON GABRIEL SANTANA SOARES CRUZ LIBOREIRO', '***.837.296-**', 'QUELE SOARES DOS ANJOS DA CRUZ', '200,00'],
        ['Detalhar', '01/2025', 'PR', 'SÃO JOSÉ DOS PINHAIS', 'AARON OLIVEIRA DOS SANTOS', '***.837.127-**', 'LUCIENE NASCIMENTO DE OLIVEIRA', '200,00'],
        ['Detalhar', '01/2025', 'SP', 'SANTANA DE PARNAÍBA', 'AARON RECOBA LESTARPE', '***.202.928-**', 'ALEJANDRA MICAELA LESTARPE', '200,00'],
        ['Detalhar', '01/2025', 'MT', 'BRASNORTE', 'AAWA MYKY', '***.691.691-**', 'MARIKANANU MYKY', '200,00'],
        ['Detalhar', '01/2025', 'BA', 'SALVADOR', 'ABADE VITORIA OLIVEIRA MARTINS MACHADO', '***.050.055-**', 'ABADE VITORIA OLIVEIRA MARTINS MACHADO', '200,00']
    ]
}

def create_initial_csv():
    """Cria o arquivo CSV inicial com os dados da primeira página"""
    headers = first_page_data['headers']
    data = first_page_data['data']
    
    # Criar DataFrame
    df = pd.DataFrame(data, columns=headers)
    
    # Salvar em CSV
    output_file = "/home/ubuntu/dados_portal_transparencia.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print("="*60)
    print("DADOS DO PORTAL DA TRANSPARÊNCIA - PROGRAMA PÉ-DE-MEIA")
    print("="*60)
    print(f"Arquivo inicial criado: {output_file}")
    print(f"Registros da primeira página: {len(df)}")
    print(f"Colunas disponíveis: {list(df.columns)}")
    
    # Mostrar estatísticas
    print(f"\nDistribuição por UF (primeira página):")
    uf_counts = df['UF'].value_counts()
    for uf, count in uf_counts.items():
        print(f"  {uf}: {count} registro(s)")
    
    # Mostrar valor total
    df_temp = df.copy()
    df_temp['Valor_Numerico'] = df_temp['Valor Disponibilizado (R$)'].str.replace(',', '.').str.replace('R$', '').str.strip()
    df_temp['Valor_Numerico'] = pd.to_numeric(df_temp['Valor_Numerico'], errors='coerce')
    total_valor = df_temp['Valor_Numerico'].sum()
    print(f"\nValor total (primeira página): R$ {total_valor:,.2f}")
    
    # Mostrar primeiras linhas
    print(f"\nPrimeiras 5 linhas dos dados:")
    print(df.head().to_string())
    
    print(f"\n" + "="*60)
    print("INSTRUÇÕES PARA CONTINUAR A COLETA:")
    print("="*60)
    print("1. No navegador, clique no botão 'próxima página' (seta para direita)")
    print("2. Aguarde a página carregar")
    print("3. Execute o comando: python append_page_data.py")
    print("4. Repita os passos 1-3 até coletar todos os dados desejados")
    print("5. O limite máximo é de 20.000 registros")
    
    return df

def show_collection_status():
    """Mostra o status atual da coleta"""
    try:
        df = pd.read_csv("/home/ubuntu/dados_portal_transparencia.csv")
        print(f"\nStatus atual da coleta:")
        print(f"Total de registros coletados: {len(df):,}")
        print(f"Progresso: {len(df)/20000*100:.1f}% (limite: 20.000 registros)")
        
        # Estatísticas por UF
        print(f"\nDistribuição por UF (Top 10):")
        uf_counts = df['UF'].value_counts().head(10)
        for uf, count in uf_counts.items():
            print(f"  {uf}: {count:,} registros")
            
    except FileNotFoundError:
        print("Arquivo CSV ainda não foi criado.")

if __name__ == "__main__":
    # Criar arquivo inicial
    df = create_initial_csv()
    
    # Mostrar status
    show_collection_status()
