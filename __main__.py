import os
import re
import duckdb
import pandas as pd


PASTA_DADOS = './Detetive/'
PAGINA_DADOS = 10

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
        self.crime_id      = 0
        self.offset        = 0
        self.qtd_registros = 0
        self.trabalhando = True
        self.ultima_func = None
        self.rascunho: pd.DataFrame = None

    def verifica_progresso(self):
        [f for f in os.listdir(PASTA_DADOS) if f.endswith('.parquet')]

    def libera_opcoes(self):
        self.MENU = {1: self.Casos_em_Aberto}
        if self.crime_id:
            self.MENU |= {
                2: self.Identifica_Suspeitos,
                3: self.Alibi_dos_Suspeitos,
                4: self.Possivel_arma_do_Crime,
                5: self.Depoimentos_inconsistentes,
            }
        if self.ultima_func:
            if self.ultima_func == self.Casos_em_Aberto:
                self.MENU |= {6: self.Pegar_um_caso}
            self.MENU |= {7: self.Mais_Resultados}
        self.MENU |= {0: self.Sair}
    
    def Mais_Resultados(self):
        """Mostra mais resultados da última consulta"""
        self.offset += PAGINA_DADOS
        pagina_final = self.qtd_registros - PAGINA_DADOS
        res = self.ultima_func()
        if self.offset >= pagina_final:
            self.offset = 0
            self.ultima_func = None
            self.libera_opcoes()
        return res

    def barra_progresso(self):
        pos_atual = self.offset + PAGINA_DADOS
        pct: int = round(
            PAGINA_DADOS * pos_atual / self.qtd_registros
        )
        formato = '[{:<' + str(PAGINA_DADOS) + '}]'
        print( formato.format('■'*pct) )

    def proximo_offset(self) -> str:
        return f'LIMIT {PAGINA_DADOS} OFFSET {self.offset}'

    def Pegar_um_caso(self):
        """Escolher um caso para trabalhar"""
        self.crime_id = 0
        while not self.crime_id:
            try:
                self.crime_id = int( input('Qual caso você vai pegar?') )
            except ValueError:
                print('Este não parece um número de caso. 😒')
        self.mostra_caso_escolhido()
        self.Casos_em_Aberto(True)

    def Casos_em_Aberto(self, filtrar_crime: bool=False):
        """Listar os casos em aberto"""
        # -------------------------------------------------
        query = f"""
            SELECT
                c.id as caso, p.nome as vitima,
                c.ocorrencia, c.local, c.lesao
            FROM {TABELA_CRIME}
                JOIN {TABELA_PESSOA} ON (c.vitima = p.id)
                
        """
        if filtrar_crime:
            query += f"WHERE c.id = {self.crime_id}"  
            # O caso atual ----------------^^^
        else:
            self.qtd_registros = duckdb.sql(
                f"SELECT Count(*) FROM {TABELA_CRIME}"
            ).fetchone()[0]
            query += 'ORDER BY c.id ' + self.proximo_offset()
        # -------------------------------------------------
        res = duckdb.sql(query)
        self.ultima_func = self.Casos_em_Aberto
        self.libera_opcoes()
        self.barra_progresso()
        return res
    
    def mostra_caso_escolhido(self):
        print('░░░▒▒▒▓▓▓ Caso {}: ▓▓▓▒▒▒░░░'.format(
            self.crime_id
        ))

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
            LIMIT {PAGINA_DADOS}
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

    def executa(self):
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
            print('Opções:'.center(50, '='))
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
            print('°º¤ø,_,ø¤º°`°º¤ø, {} ,ø¤°º¤ø,_,ø¤º°`°º¤ø,_'.format(
                funcao.__name__
            ))
            print( funcao() )



if __name__ == '__main__':
    jogo = JogoDetetive()
    jogo.executa()
 