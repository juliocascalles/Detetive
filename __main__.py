import duckdb
import re


PASTA_DADOS = './Detetive/'

TABELA_DEPOIMENTO = f"'{PASTA_DADOS}Depoimento.parquet' d"
TABELA_PESSOA     = f"'{PASTA_DADOS}Pessoa.parquet'     p"
TABELA_OBJETO     = f"'{PASTA_DADOS}Objeto.parquet'     o"
TABELA_CRIME      = f"'{PASTA_DADOS}Crime.parquet'      c"
TABELA_SUSPEITO   = f"'{PASTA_DADOS}Suspeito.parquet'   s"
TABELA_SUSPEITO_ = TABELA_SUSPEITO.split()[0]


class JogoDetetive:

    def __init__(self):
        self.MENU = { 
            1: self.Casos_em_Aberto,
        }
        self.crime_id = 0
        self.trabalhando = True

    def libera_opcoes(self):
        self.MENU = {
            1: self.Casos_em_Aberto,
            2: self.Identifica_Suspeitos,
            3: self.Alibi_dos_Suspeitos,
            4: self.Possivel_arma_do_Crime,
            5: self.Depoimentos_inconsistentes,
            0: self.Sair,
        }
    
    def Casos_em_Aberto(self):
        """Listar os casos em aberto"""
        query = f"""
            SELECT
                c.id as caso, p.nome as vitima,
                c.ocorrencia, c.local, c.lesao
            FROM {TABELA_CRIME}
                JOIN {TABELA_PESSOA} ON (c.vitima = p.id)
        """
        res = duckdb.sql(query+'\n LIMIT 10')
        print(res)
        escolha = 0
        while escolha in (0, self.crime_id):
            try:
                escolha = int( input('Qual caso você vai pegar?') )
            except ValueError:
                print('Este não parece um número de caso. 😒')
        self.crime_id = escolha
        query += f"""
            WHERE c.id = {self.crime_id}
        """
        print('°º¤ø,_,ø¤º°`°º¤ø, Caso escolhido: ,ø¤°º¤ø,_,ø¤º°`°º¤ø,_')
        self.libera_opcoes()
        return duckdb.sql(query)

    def Alibi_dos_Suspeitos(self):
        """Retorna quais suspeitos tem álibi para a hora do crime"""
        query = f"""
            SELECT
                    c.local as local_crime,
                    d.suspeito,
                    p.nome as nome_testemunha,
                    d.ocorrencia,
                    d.local as onde_suspeito_estava
            FROM
                    {TABELA_DEPOIMENTO}
                    JOIN {TABELA_PESSOA}   ON (d.testemunha = p.id)
                    JOIN {TABELA_SUSPEITO} ON (d.suspeito = s.id)
                    JOIN {TABELA_CRIME}    ON (s.crime = c.id)
            WHERE
                    s.crime = {self.crime_id} AND
                    d.ocorrencia = c.ocorrencia AND
                    d.local <> c.local
        """
        return duckdb.sql(query)

    def Identifica_Suspeitos(self):
        """Mostra as pessoas parecidas com a descrição do suspeito"""
        query = f"""
            SELECT
                    p.id, p.nome, 
                    CASE
                        WHEN s.cabelo = p.cabelo THEN '   cabelo    '
                        WHEN s.olhos  = p.olhos THEN  '    olhos    '
                                                 ELSE 'peso e altura'
                    END as similaridade
            FROM
                    {TABELA_SUSPEITO}
                    JOIN {TABELA_PESSOA}
                    ON ( 
                        s.sexo = p.sexo
                        AND
                        (
                            (
                                ABS(s.altura - p.altura) < 1
                                AND  ABS(s.peso - p.peso) < 6
                            )
                            AND (s.cabelo = p.cabelo OR  s.olhos = p.olhos)
                        )
                    )
            WHERE
                    s.crime = {self.crime_id}
            ORDER BY
                    p.nome
        """
        return duckdb.sql(query)

    def Possivel_arma_do_Crime(self):
        """Donos de objetos similares à arma do crime"""
        query = f"""
            SELECT
                    p.id, o.tipo,
                    p.nome
            FROM
                    {TABELA_OBJETO}
                    JOIN {TABELA_CRIME}  ON (o.crime = c.id)
                    JOIN {TABELA_PESSOA} ON (o.dono = p.id)
            WHERE
                    c.lesao = o.lesao AND
                    o.crime = {self.crime_id}
        """
        return duckdb.sql(query)

    def Depoimentos_inconsistentes(self):
        """
        Depoimentos muito diferentes de outros para o mesmo crime
        """
        # ------------------------------------------------
        
        def sub_select(campo: str, alias: str, sinal: str) -> str:        
            FATOR_DESVIO = 1.0
            return re.sub(r'\s+', ' ', f"""(
                SELECT AVG({alias}.{campo})
                {sinal} ({FATOR_DESVIO:.2f} * STDDEV({alias}.{campo}))
                FROM {TABELA_SUSPEITO_} {alias}
                WHERE {alias}.crime = {self.crime_id}
            )
            """)
        def agrupa_por(campo: str, alias: str) -> str:
            return f"""
                SELECT
                    '{campo}' as inconsistencia,
                    {alias}.{campo} relatado
                FROM
                    {TABELA_SUSPEITO_} {alias}
                WHERE
                    {alias}.crime = {self.crime_id}
                GROUP BY
                    {alias}.{campo} HAVING Count(*) < 2        
            """
        def desvio_padrao(campo: str, alias: str) -> str:
            return f"""
                SELECT
                    '{campo}' as inconsistencia,
                    {alias}.{campo} relatado
                FROM
                    {TABELA_SUSPEITO_} {alias}
                WHERE
                    {alias}.crime = {self.crime_id}
                    AND (
                        {alias}.peso > {sub_select(campo, 's2', '+')}
                        OR
                        {alias}.peso < {sub_select(campo, 's3', '-')}
                    )
            ;"""
        lista = [
            agrupa_por('cabelo', 'g1'), agrupa_por('sexo', 'g1'),
            desvio_padrao('peso', 'd1')
        ]
        query = '\nUNION ALL\n'.join(lista)
        return duckdb.sql(query)

    def Sair(self):
        """Encerra o expediente por hoje. Volte para casa, tenente..."""
        self.trabalhando = False
        return '''
      ______                     _              __                 _         
     /_  __/__     _   _____    (_)___     ____/ /__  ____  ____  (_)____    
      / / / _ \\   | | / / _ \\  / / __ \\   / __  / _ \\/ __ \\/ __ \\/ / ___/    
     / / /  __/   | |/ /  __/ / / /_/ /  / /_/ /  __/ /_/ / /_/ / (__  ) _ _ 
    /_/  \\___/    |___/\\___/_/ /\\____/   \\__,_/\\___/ .___/\\____/_/____(_|_|_)
                          /___/                   /_/                        
        '''

    def mostra_menu(self):
        print("""
            ██████╗ ███████╗████████╗███████╗████████╗██╗██╗   ██╗███████╗
            ██╔══██╗██╔════╝╚══██╔══╝██╔════╝╚══██╔══╝██║██║   ██║██╔════╝
            ██║  ██║█████╗     ██║   █████╗     ██║   ██║██║   ██║█████╗  
            ██║  ██║██╔══╝     ██║   ██╔══╝     ██║   ██║╚██╗ ██╔╝██╔══╝  
            ██████╔╝███████╗   ██║   ███████╗   ██║   ██║ ╚████╔╝ ███████╗
            ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝   ╚═╝   ╚═╝  ╚═══╝  ╚══════╝
        """)
        
        print('''
        +------------------------------------------------------------+
            
                    Olá, tenente! O capitão Pedroza está furioso
                porque os casos de homicídio estão se acumulando!
                
                    Precisamos dar andamento nas investigações.
        +------------------------------------------------------------+

        ''')
        while self.trabalhando:
            for op, func in self.MENU.items():
                print(f'{op} - {func.__doc__.strip()}')
            opcao = input('\n ... O que deseja fazer? ')
            try:
                funcao = self.MENU[ int(opcao) ]
            except ValueError:
                print(",.-~*´¨¯¨`*·~-._-(_Você deve digitar um NÚMERO._)-,.-~*´¨¯¨`*·~-.¸")
                continue
            except KeyError:
                print(",.-~*´¨¯¨`*·~-._-( Esta opção não existe. )-,.-~*´¨¯¨`*·~-.¸")
                continue
            print('░░░▒▒▒▓▓▓ ', funcao.__name__, '▓▓▓▒▒▒░░░')
            print( funcao() )



if __name__ == '__main__':
    jogo = JogoDetetive()
    jogo.mostra_menu()
 