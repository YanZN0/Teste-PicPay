import asyncio
import aiohttp
import pandas as pd


async def fetch(session, url):
    """Função auxiliar para fazer requisições assíncronas."""
    async with session.get(url) as response:
        return await response.json()


async def exibir_pokemons_e_tipos() -> pd.DataFrame:
    pokemons = []
    url = "https://pokeapi.co/api/v2/pokemon/"
    
    async with aiohttp.ClientSession() as session:
        while url:
            data = await fetch(session, url)
            pokemons.extend(data["results"])
            url = data['next']
        
        lista_pokemons = []
        for pokemon in pokemons:
            pokemon_data = await fetch(session, pokemon['url'])
            
            if pokemon_data.get('is_default', True):
                nome = pokemon_data['name']
                types = [type_info['type']['name'] for type_info in pokemon_data['types']]
                lista_pokemons.append({'name': nome, 'types': types})
    return pd.DataFrame(lista_pokemons)


def filtrar_pokemons_do_tipo_gelo(df):
    data = df[df['types'].apply(lambda tipos: 'ice' in tipos)]
    return data


async def pokemon_experience(df: pd.DataFrame) -> pd.DataFrame:
    experience_data = []
    async with aiohttp.ClientSession() as session:
        for _, row in df.iterrows():
            pokemon_data = await fetch(session, f"https://pokeapi.co/api/v2/pokemon/{row['name']}")
            experience_data.append(pokemon_data['base_experience'])
    
    df = df.copy()
    df.loc[:, 'experience'] = experience_data
    return df


async def pokemon_de_gelo_drop_maior_experience(df: pd.DataFrame) -> pd.DataFrame:
    df_ice = filtrar_pokemons_do_tipo_gelo(df)
    df_ice_com_experience = await pokemon_experience(df_ice)
    pokemon_top_experience = df_ice_com_experience.loc[df_ice_com_experience['experience'].idxmax()]
    df_atualizado = pd.DataFrame([pokemon_top_experience])
    df_top = df_atualizado.head(2)
    return df_top


async def pipeline_pokemon_ice_experience():
    pokemons = await exibir_pokemons_e_tipos()
    filtragem = filtrar_pokemons_do_tipo_gelo(pokemons)
    experience = await pokemon_experience(filtragem)
    pokemon_maior_experience = await pokemon_de_gelo_drop_maior_experience(experience)
    return pokemon_maior_experience


# Executa a pipeline
if __name__ == "__main__":
    result = asyncio.run(pipeline_pokemon_ice_experience())
    print(result)
