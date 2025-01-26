from query_pokemon_5 import pipeline_query5
import asyncio

df_final = asyncio.run(pipeline_query5())
print(df_final)
