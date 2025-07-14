import networkx as nx
import matplotlib.pyplot as plt

# Assuming 'result' is your list of (table_name, esp_job_id) tuples
G = nx.DiGraph()
for table, job in result:
    label = f"{table}\n{job}" if job else table
    G.add_node(label)
# If you have parent-child relationships, add edges accordingly
# For a simple chain, you can connect them in order:
for i in range(1, len(result)):
    prev_label = f"{result[i-1][0]}\n{result[i-1][1]}" if result[i-1][1] else result[i-1][0]
    curr_label = f"{result[i][0]}\n{result[i][1]}" if result[i][1] else result[i][0]
    G.add_edge(prev_label, curr_label)

plt.figure(figsize=(10, 6))
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=2000, font_size=10, arrows=True)
plt.title("Job Lineage")
plt.show()
