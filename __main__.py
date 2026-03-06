import os
import re
import duckdb
import pandas as pd
from enum import Enum


PASTA_DADOS   = './Detetive/'
TABELA_DEPOIMENTO = f"'{PASTA_DADOS}Depoimento.parquet' d"
TABELA_PESSOA     = f"'{PASTA_DADOS}Pessoa.parquet'     p"
TABELA_OBJETO     = f"'{PASTA_DADOS}Objeto.parquet'     o"
TABELA_CRIME      = f"'{PASTA_DADOS}Crime.parquet'      c"
TABELA_SUSPEITO   = f"'{PASTA_DADOS}Suspeito.parquet'   s"
TABELA_SUSPEITO_ = TABELA_SUSPEITO.split()[0]
ARQUIVO_ANOTACOES = f'{PASTA_DADOS}/anotacoes.csv'


class Filtro(Enum):
    NENHUM     = 0
    CASO_ATUAL = 1
    CONTAGEM   = 2


class JogoDetetive:

    def __init__(self):
        self.crime_id      = 0
        self.offset        = 0
        self.qtd_registros = 0
        self.trabalhando = True
        self.ultima_func = None
        self.rascunho: pd.DataFrame = None  # [To-Do] >>> Gravar o progresso do "tenente Falcão"
        # ^^^^^^^^^^^^^------------------------------- self.carrega_anotacoes(...)
        self.listando_casos: bool = False
        self.carrega_anotacoes()
        self.habilita_opcoes()
    
    def configurar_paginacao(self, funcao: callable):
        FUNCOES_NAVEGAVEIS = [
            self.Casos_em_Aberto,
            self.Identifica_Suspeitos,
            self.Alibi_dos_Suspeitos,
            self.Possivel_arma_do_Crime,
            self.Depoimentos_inconsistentes,
        ]
        if funcao not in FUNCOES_NAVEGAVEIS:
            return
        self.ultima_func = funcao
        self.listando_casos = (funcao == self.Casos_em_Aberto)
        self.habilita_opcoes()
        self.qtd_registros = funcao(Filtro.CONTAGEM).fetchone()[0]
        acima_de200: bool = (self.qtd_registros > 200)
        self.TAMANHO_PAGINA = 25 if acima_de200 else 10
        self.barra_progresso()
        self.offset = 0

    def carrega_anotacoes(self):
        if os.path.exists(ARQUIVO_ANOTACOES):
            self.rascunho = pd.read_csv(ARQUIVO_ANOTACOES)

    def habilita_opcoes(self):
        self.MENU = {1: self.Casos_em_Aberto}
        if self.crime_id:
            self.MENU |= {
                2: self.Identifica_Suspeitos,
                3: self.Alibi_dos_Suspeitos,
                4: self.Possivel_arma_do_Crime,
                5: self.Depoimentos_inconsistentes,                
            }
        if self.listando_casos:
            self.MENU |= {6: self.Pegar_um_caso}
        if self.ultima_func:
            self.MENU |= {7: self.Mais_Resultados}
        if self.rascunho:
            self.MENU |= {8: self.Elimina_Pistas_Falsas}
        self.MENU |= {0: self.Sair}
    
    def Elimina_Pistas_Falsas(self):
        """Baseado na última consulta, refaz as anotações do caso (🚧👷🏼‍♂️ EM CONSTRUÇÃO 👷🏼‍♀️🏗️)"""
        return self.rascunho

    def Mais_Resultados(self):
        """Mostra mais resultados da última consulta"""
        self.offset += self.TAMANHO_PAGINA
        pagina_final = self.qtd_registros - self.TAMANHO_PAGINA
        res = self.ultima_func()
        self.barra_progresso()
        if self.offset >= pagina_final:
            self.limpa_offset()
        return res

    def limpa_offset(self):
        self.offset = 0
        self.ultima_func = None
        self.habilita_opcoes()

    def barra_progresso(self):
        if self.qtd_registros < self.TAMANHO_PAGINA:
            pct = self.TAMANHO_PAGINA
        else:
            pos_atual = self.offset + self.TAMANHO_PAGINA
            pct: int = round(
                self.TAMANHO_PAGINA * pos_atual / self.qtd_registros
            )
            print('Página {} de {}'.format(
                round(self.offset / self.TAMANHO_PAGINA) + 1,
                round(self.qtd_registros / self.TAMANHO_PAGINA)
            ))
        print( '[{}{}]'.format(
            '■'*pct,
            (self.TAMANHO_PAGINA - pct) * ' '
        ))

    def proximo_offset(self) -> str:
        return f'LIMIT {self.TAMANHO_PAGINA} OFFSET {self.offset}'

    def Pegar_um_caso(self):
        """Escolher um caso para trabalhar"""
        self.crime_id = 0
        while not self.crime_id:
            try:
                self.crime_id = int( input('Qual caso você vai pegar? ') )
            except ValueError:
                print('Este não parece um número de caso. 😒')
        self.mostra_caso_escolhido()
        self.listando_casos = False
        self.limpa_offset()        
        return self.Casos_em_Aberto(Filtro.CASO_ATUAL)

    def Casos_em_Aberto(self, filtro: Filtro = Filtro.NENHUM):
        """Listar os casos em aberto"""
        # -------------------------------------------------
        if filtro == Filtro.CONTAGEM:
            CAMPOS = 'Count(*)'
        else:
            CAMPOS = '''
                c.id as caso, p.nome as vitima,
                c.ocorrencia, c.local, c.lesao
            '''
        # ---------------------------------------------------
        query = f"""
            SELECT
                {CAMPOS}
            FROM {TABELA_CRIME}
                JOIN {TABELA_PESSOA} ON (c.vitima = p.id)
                
        """
        if filtro == Filtro.CASO_ATUAL:
            query += f"WHERE c.id = {self.crime_id}"  
            # O caso atual ----------------^^^
        elif filtro == Filtro.NENHUM:
            query += 'ORDER BY c.id ' + self.proximo_offset()
        # -------------------------------------------------
        res = duckdb.sql(query)
        return res
    
    def mostra_caso_escolhido(self):
        print('░░░▒▒▒▓▓▓ Caso {}: ▓▓▓▒▒▒░░░'.format(
            self.crime_id
        ))

    def Alibi_dos_Suspeitos(self, filtro: Filtro=Filtro.NENHUM):
        """Retorna quais suspeitos tem álibi para a hora do crime"""
        # ------------------------------------------------------------
        if filtro == Filtro.CONTAGEM:
            CAMPOS = 'Count(*)'
        else:
            CAMPOS = """
                c.local as local_crime,
                d.suspeito,
                p.nome as nome_testemunha,
                d.ocorrencia,
                d.local as onde_suspeito_estava
            """
        query = f"""
            SELECT
                {CAMPOS}
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
        if filtro == Filtro.NENHUM:
            query += self.proximo_offset()
        return duckdb.sql(query)

    def Identifica_Suspeitos(self, filtro: Filtro = Filtro.NENHUM):
        """Mostra as pessoas parecidas com a descrição do suspeito"""
        # --------------------------------------------------------------
        if filtro == Filtro.CONTAGEM:
            CAMPOS = 'Count(*)'
        else:
            CAMPOS = """
                p.id, p.nome, 
                CASE
                    WHEN s.cabelo = p.cabelo THEN '   cabelo    '
                    WHEN s.olhos  = p.olhos THEN  '    olhos    '
                                            ELSE 'peso e altura'
                END as similaridade
            """
        query = f"""
            SELECT
                {CAMPOS}
            FROM
                {TABELA_SUSPEITO}                
                JOIN {TABELA_PESSOA}
                ON ( 
                    s.sexo = p.sexo AND
                    (
                        (
                            ABS(s.altura - p.altura) < 0.5 AND
                            ABS(s.peso - p.peso) < 6
                        )
                        OR
                        s.cabelo = p.cabelo
                        OR
                        s.olhos = p.olhos
                    )
                )
            WHERE
                s.crime = {self.crime_id}
        """
        if filtro == Filtro.NENHUM:
            # query += 'ORDER BY p.nome '
            query += self.proximo_offset()
        return duckdb.sql(query)

    def Possivel_arma_do_Crime(self, filtro: Filtro = Filtro.NENHUM):
        """Donos de objetos similares à arma do crime"""
        # -----------------------------------------------------
        if filtro == Filtro.CONTAGEM:
            CAMPOS = 'Count(*)'
        else:
            CAMPOS = '''
                p.id, o.tipo,
                p.nome
            '''
        query = f"""
            SELECT
                {CAMPOS}
            FROM
                {TABELA_OBJETO}
                JOIN {TABELA_CRIME}  ON (o.crime = c.id)
                JOIN {TABELA_PESSOA} ON (o.dono = p.id)
            WHERE
                c.lesao = o.lesao AND
                o.crime = {self.crime_id}
        """
        if filtro == Filtro.NENHUM:
            query += self.proximo_offset()
        return duckdb.sql(query)

    def Depoimentos_inconsistentes(self, filtro: Filtro = Filtro.NENHUM):
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
            """
        lista = [
            agrupa_por('cabelo', 'g1'), agrupa_por('sexo', 'g1'),
            desvio_padrao('peso', 'd1')
        ]
        query = '''WITH Resultado AS (
            {}
        )SELECT {} FROM Resultado
        '''.format(
            '\nUNION ALL\n'.join(lista),
            'Count(*)' if filtro == Filtro.CONTAGEM else 'inconsistencia, relatado'
        )
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
            opcao = input('\n ... O que deseja fazer?🤔 ')
            try:
                funcao = self.MENU[ int(opcao) ]
            except ValueError:
                print(",.-~*´¨¯¨`*·~-._-( ❌ Você deve digitar um NÚMERO.)-,.-~*´¨¯¨`*·~-.¸")
                continue
            except KeyError:
                print(",.-~*´¨¯¨`*·~-._-( ❌ Esta opção não existe. )-,.-~*´¨¯¨`*·~-.¸")
                continue
            print('°º¤ø,_,ø¤º°`°º¤ø, {} ,ø¤°º¤ø,_,ø¤º°`°º¤ø,_'.format(
                funcao.__name__
            ))
            self.configurar_paginacao(funcao)
            print( funcao() )



if __name__ == '__main__':
    jogo = JogoDetetive()
    jogo.executa()
 