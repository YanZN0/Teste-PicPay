from pokemon_ice_experience import pipeline_pokemon_ice_experience
import asyncio

if __name__ == "__main__":
    result = asyncio.run(pipeline_pokemon_ice_experience())
    print(result)
