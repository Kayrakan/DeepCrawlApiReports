

import asyncio
import helpers

async def main():
    """ gathers tasks and run. Currently, there is only one task
     here but multiple can be added and run all together."""

    task1 = asyncio.create_task(helpers.start_deepcrawl())

    await task1

if __name__ == '__main__':
    asyncio.run(main())

