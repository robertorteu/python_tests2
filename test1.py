import pandas as pd


print("Hello World")
print("How are you doing?")
print("Mangueta a full")

# Read the Excel file
data = pd.read_excel(r'C:\Users\RobertOrteu\OneDrive - Propelling Tech S.L\Documents\python_tests\dataset1.xlsx')

# Filter the data by Location = "Lleida"
filtered_data = data[data['Location'] == 'Lleida']

# Calculate the sum of the Amounts column
total_amount = filtered_data['Amount'].sum()

# Print the result
print(total_amount)
# Generate a new Excel file with the total_amount value
filtered_data['Total_Amount'] = total_amount
output_file = 'C:\\Users\\RobertOrteu\\OneDrive - Propelling Tech S.L\\Documents\\python_tests\\dataset1_outcome.xlsx'
filtered_data.to_excel(output_file, index=False)
print(total_amount)
print("I am a hacker")