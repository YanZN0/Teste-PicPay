import asyncio
import aiohttp
import pandas as pd

# Dicionário de vantagens de ataque por tipo
vantagens_atacantes = {
    'normal': ['ghost'],  # Normal não tem vantagem, só exemplo de ineficiência
    'fire': ['grass', 'bug', 'ice', 'steel'],
    'water': ['fire', 'ground', 'rock'],
    'electric': ['water', 'flying'],
    'grass': ['water', 'ground', 'rock'],
    'ice': ['grass', 'ground', 'flying', 'dragon'],
    'fighting': ['normal', 'rock', 'steel', 'ice', 'dark'],
    'poison': ['grass', 'fairy'],
    'ground': ['fire', 'electric', 'poison', 'rock', 'steel'],
    'flying': ['fighting', 'bug', 'grass'],
    'psychic': ['fighting', 'poison'],
    'bug': ['grass', 'psychic', 'dark'],
    'rock': ['fire', 'ice', 'flying', 'bug'],
    'ghost': ['psychic', 'ghost'],
    'dragon': ['dragon'],
    'dark': ['psychic', 'ghost'],
    'steel': ['ice', 'rock', 'fairy'],
    'fairy': ['fighting', 'dragon', 'dark']
}

async def obter_pokemons():
    async with aiohttp.ClientSession() as session:
        pokemons = []
        url = "https://pokeapi.co/api/v2/pokemon/"
        
        while url:
            async with session.get(url) as response:
                data = await response.json()
                pokemons.extend(data["results"])
                url = data['next']

        # Obter detalhes de cada Pokémon e filtrar por forma padrão
        pokemons_detalhados = []
        for pokemon in pokemons:
            async with session.get(pokemon['url']) as response:
                pokemon_data = await response.json()
                if pokemon_data['is_default']:  # Considera apenas a forma padrão
                    nome = pokemon_data['name']
                    tipos = [tipo['type']['name'] for tipo in pokemon_data['types']]
                    pokemons_detalhados.append({'name': nome, 'types': tipos})

        return pd.DataFrame(pokemons_detalhados)

def calcular_vantagens(df_pokemons):
    # Armazena a quantidade de Pokémons sobre os quais cada Pokémon tem vantagem
    vantagens_por_pokemon = []

    for i, atacante in df_pokemons.iterrows():
        vantagem_contador = 0

        for j, defensor in df_pokemons.iterrows():
            if i == j:
                continue  # Não comparar um Pokémon consigo mesmo
            
            # Verificar vantagem atacante
            if any(
                tipo_atacante in vantagens_atacantes and tipo_defensor in vantagens_atacantes[tipo_atacante]
                for tipo_atacante in atacante['types']
                for tipo_defensor in defensor['types']
            ):
                vantagem_contador += 1

        vantagens_por_pokemon.append({'name': atacante['name'], 'vantagem_sobre': vantagem_contador})

    # Retorna um DataFrame com os resultados
    return pd.DataFrame(vantagens_por_pokemon).sort_values(by='vantagem_sobre', ascending=False)

async def pipeline_task():
    # Obter os Pokémons em sua forma padrão
    df_pokemons = await obter_pokemons()

    # Calcular vantagens
    df_vantagens = calcular_vantagens(df_pokemons).head(7)

    # Exibir o resultado
    print(df_vantagens)

# Executar a pipeline
asyncio.run(pipeline_task())
