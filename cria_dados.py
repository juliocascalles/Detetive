from datetime import datetime
from enum import Enum
from faker import Faker
from random import randint, choice, uniform
from duck_model import DuckModel


fake = Faker('pt_BR')
DuckModel.use_suffix = False


class Sexo(Enum):
    Masculino = 'M'
    Feminino = 'F'


class Cabelo(Enum):
    Castanho = "castanho"
    Loiro = "loiro"
    Preto = "preto"
    Grisalho = "grisalho"
    Ruivo = "ruivo"

class Olho(Enum):
    Azul = "azul"
    Castanho = "castanho"
    Verde = "verde"


class Perfil(DuckModel):
    @classmethod
    def schema(cls) -> dict:
        return dict(
            id=int, altura=float,
            peso=float, cabelo=Cabelo,
            olhos=Olho, sexo=Sexo
        )
    
    @classmethod
    def dados_fake(cls) -> dict:
        altura = uniform(1.50, 2.10)
        imc = choice([17]+[20]*5+[26]*3+[35])
        return dict(
            # nome=fake.name,
            altura=altura,
            peso=(imc * altura**2),
            cabelo=choice( list(Cabelo) ),
            olhos=choice( list(Olho) ),
            sexo=choice( list(Sexo) )
        )
    
    @classmethod
    def popula(cls, quantidade: int):
        class_name = cls.__name__
        if class_name == 'Perfil':
            raise NotImplemented('''
                (56) Perfil não é uma classe válida para criar objetos.
                Em vez disso, use uma de suas sub-classes 
                        * Pessoa
                        * Suspeito
            ''')
        while len(cls.objects) < quantidade:
            dados = cls.dados_fake()
            cls(**dados)
            print(class_name[0], end='')



class Pessoa(Perfil):
    objects = {}

    @classmethod
    def dados_fake(cls) -> dict:
        dados = super().dados_fake()
        sexo = dados['sexo']
        func_nome = {
            Sexo.Masculino: fake.name_male,
            Sexo.Feminino: fake.name_female,
        }[sexo]
        dados['nome'] = func_nome()
        return dados

    @classmethod
    def schema(cls) -> dict:
        return dict(nome=str) | super().schema()


class Local(Enum):
    Escritorio  = "Escritorio"
    Casa        = "Casa"
    Aeroporto   = "Aeroporto"
    Biblioteca  = "Biblioteca"
    Restaurante = "Restaurante"


class Lesao(Enum):
    Perfurante = "Perfurante"
    Contundente = "Contundente"
    Asfixia = "Asfixia"


class Crime(DuckModel):
    """
    Para cada crime...
        * 1 pessoa é a vítima
        * 4 pessoas são os suspeitos
        * 5 pessoas são as testemunhas
    """
    objects = {}

    @classmethod
    def schema(cls) -> dict:
        return dict(
            id=int,
            vitima=int,
            ocorrencia=datetime,
            local=Local,
            lesao=Lesao, # Causa-mortis: Perfuração, Golpe contundente...
        )
    
    @classmethod
    def popula(cls, quantidade: int):
        if not Pessoa.objects:
            raise ValueError('(122) Não existem pessoas para relacionar com suspeitos.')
        while len(cls.objects) < quantidade:
            pessoa = choice( list(Pessoa.objects.values()) )
            cls(
                vitima=pessoa.id,
                ocorrencia=fake.date_time_between(
                    start_date='-1y', end_date=datetime.today()
                ),
                local=choice( list(Local) ),
                lesao=choice( list(Lesao) )
            )
            print('C', end='')

    @classmethod
    def envolvimento(cls, pessoa: Pessoa) -> bool:
        """
        Retorna se a pessoa está envolvida
        em algum crime, como vítima ou suspeito
        """
        for crime in cls.objects.values():
            if pessoa.id == crime.vitima:
                return True
        return pessoa.id in Suspeito.objects


class Depoimento(DuckModel):
    """
    Alguns depoimentos incriminam os suspeitos,
    outros, fornecem álibis para eles.
    
    Exemplo de Álibi:
    * O suspeito estava em outro local neste mesmo horário
    """
    objects = {}

    @classmethod
    def schema(cls) -> dict:
        return dict(
            id=int,
            testemunha=int, # Liga com Pessoa.id
            suspeito=int,
            ocorrencia=datetime,
            local=Local,
        )
    
    @classmethod
    def popula(cls, quantidade: int):
        if not Pessoa.objects:
            raise ValueError('(169) Não existem testemunhas para os depoimentos.')
        if not Suspeito.objects:
            raise ValueError('(171) Não existem suspeitos para os depoimentos.')
        possiveis_testemunhas = [
            p.id for p in Pessoa.objects.values() if not Crime.envolvimento(p)
        ]
        while len(cls.objects) < quantidade:
            alibi = choice([True]*4 + [False]*6) # 40% de chance de ser Álibi
            suspeito=choice( list(Suspeito.objects.values()) )
            """
            Se for Álibi, o suspeito estará em outro lugar no mesmo horário do crime:
            """
            if alibi:
                crime = Crime.objects[ suspeito.crime ]
                ocorrencia = crime.ocorrencia
                locais = [local for local in Local if local != crime.local]
            else:
                ocorrencia=fake.date_time_between(
                    start_date='-1y', end_date=datetime.today()
                )
                locais = list(Local) 
            cls(
                testemunha=choice(possiveis_testemunhas),
                suspeito=suspeito.id,
                ocorrencia=ocorrencia,
                local=choice(locais)
            )
            print('{}'.format(
                'A' if alibi else 'D'
            ), end='')


LESAO_POR_OBJETO = {
    Lesao.Perfurante: 'Faca',
    Lesao.Contundente: 'Martelo',
    Lesao.Asfixia: 'Corda',
}

class Objeto(DuckModel):
    """
    Objetos encontrados na cena do crime
    """
    objects = {}

    @classmethod
    def schema(cls) -> dict:
        return dict(
            id=int,
            crime=int,
            tipo=str, 
            dono=int,  # A qual `Pessoa` pertence esse objeto
            lesao=Lesao, # Tipo de lesão que o objeto pode causar
        )
    
    @classmethod
    def cria_objeto(cls, crime: Crime, pessoa: Pessoa, pista:bool):
        if pista:
            lesao = crime.lesao
        else:
            lesao = choice( list(Lesao) )
        cls(
            crime=crime.id,
            tipo=LESAO_POR_OBJETO[lesao],
            dono=pessoa.id,
            lesao=lesao,
        )
        print('{}'.format(
            'i' if pista else 'O'
        ), end='')


class Suspeito(Perfil):
    objects = {}

    @classmethod
    def dados_fake(cls) -> dict:
        if not Crime.objects:
            raise ValueError('(225) Não existem crimes para relacionar com suspeitos.')
        possiveis_suspeitos = [p for p in Pessoa.objects.values() if not Crime.envolvimento(p)]
        if not possiveis_suspeitos:
            raise ValueError('(228) Não existem pessoas que possam ser suspeitos.')
        dados = super().dados_fake()
        crime = choice( list(Crime.objects.values()) )
        dados['crime'] = crime.id
        pessoa = choice(possiveis_suspeitos)
        dados |= dict(
            # pessoa=pessoa,  # Suspeito não é uma pessoa, é um PERFIL
            cabelo=pessoa.cabelo,
            olhos=pessoa.olhos, sexo=pessoa.sexo,
            peso=pessoa.peso + randint(-5, 5),
            altura=pessoa.altura + uniform(-0.5, 0.5),
        )
        pista = choice([True] * 40 + [False] * 60) # 40% de chance do objeto ser uma pista do crime!
        Objeto.cria_objeto(crime, pessoa, pista)
        return dados

    @classmethod
    def schema(cls) -> dict:
        return dict(crime=int) | super().schema()


if __name__ == '__main__':
    print('Criando >> ')
    Pessoa.popula(2000)
    Crime.popula(50)
    Suspeito.popula(200)  # --- Cria os objetos também!
    Depoimento.popula(300)
    print('\n---- Gravando dados... -------------------')
    Pessoa.save()
    Crime.save()
    Suspeito.save()
    Depoimento.save()
    Objeto.save()
    print('##### Processo concluído! ################')
