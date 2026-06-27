
from typing import Dict, Optional
from chia.base.tools.ChiaTool import ChiaTool

class ChiaToolTemplate(ChiaTool):

    # This legacy method is still supported.
    # def __init__(self, name: str, task_options: Optional[Dict] = None):
    #     """
    #     Initialize the new Chia tool with the given name and resource 
    #     requirements.

    #     Name will be used in the MCP router and for logging purposes.

    #     Resource requirements should be a dictionary specifying the names of 
    #     resources the tool requires such that Ray puts the tool on a correct
    #     node.
    #     """
    #     super().__init__(name, task_options)

    #     # Must add tool methods to mcp after calling super().__init__()
    #     # because self.mcp is created in the base classes __init__ method
    #     self.mcp.add_tool(self.hello_world_tool, name=f"{self.name}_hello_world_tool")
    #     self.mcp.add_tool(self.goodbye_world_tool, name=f"{self.name}_goodbye_world_tool")

    #     # Must call super().__post_init__() after adding tools to ensure
    #     # the tool is added to the MCP router with all its tools.
    #     super().__post_init__()


    def setup(self):
        """
        Setup function for the CHIA tool
        """
        # Must add tool methods to mcp after calling super().__init__()
        # because self.mcp is created in the base classes __init__ method
        self.mcp.add_tool(self.hello_world_tool, name=f"{self.name}_hello_world_tool")
        self.mcp.add_tool(self.goodbye_world_tool, name=f"{self.name}_goodbye_world_tool")


    # Example tool methods
    # Tool methods should have doc strings describing what the tool does, and
    # the arguments it takes, so they can be called correctly by the agent.
    def hello_world_tool(self, name: str) -> str:
        """
        Example tool method that takes a name as input and returns a greeting string.

        Args:
            name (str): The name to include in the greeting.
        """
        return f"Hello, {name}! This is {self.name}."
    def goodbye_world_tool(self, name: str) -> str:
        """
        Example tool method that takes a name as input and returns a goodbye string.

        Args:
            name (str): The name to include in the goodbye message.
        """
        return f"Goodbye, {name}! This is {self.name} signing off."

# Create the tool
# ChiaToolTemplate("Hello_tool", task_options=None)