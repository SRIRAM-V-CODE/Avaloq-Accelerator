import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import pandas as pd


def convScrToFile(file_path):
    screen = open(file_path, 'r')
    f_input = input("ENTER OUTPUT REP FILE NAME: ").strip()+".txt"
    file = open(f_input,'w')
    f = screen.readlines()
    for line in f:
        if "user_id" in line:
            words = [val.strip() for val in line.split()]
            val = words[1]
            file.write(line.replace(val[1:-1], val[1:-4]))
            continue
        if "import" not in line:
            file.write(line)
        else:
            pos = f.index(line)
            break
    while pos:
        if "import" not in f[pos]:
            break
        else:
            file.write(f[pos])
        pos += 1
    while pos:
        if "screen layout" in f[pos]:
            break
        file.write(f[pos])
        pos += 1
    file.write(f[pos].replace("screen", "file"))
    pos += 2
    file.write("\ttype mba$intf$out_mmtd_02_bu_ph_pb\n")
    file.write("\tfilename 'AVQ_TDP_02_' || to_char(sysdate, mba$date.c_fmt) || '_' || '%%seq%%' || '.csv'\n")
    while pos<len(f):
        if "on column head" not in f[pos]:
            file.write(f[pos])
            pos += 1
        else:
            break
    file.write(f[pos].replace("column", "report"))
    pos += 1
    while pos<len(f):
        if "column " not in f[pos]:
            file.write(f[pos])
            pos += 1
        else:
            break
    flag=0
    while pos<len(f):
        if "column" in f[pos]:
            inner = pos + 1
            while inner<len(f):
                if "label" in f[inner]:
                    break
                else:
                    inner += 1
            if inner<len(f):
                pos = inner
                words = [val.strip() for val in f[pos].split()]
                val = words[1]
                if val.startswith("session.text"):
                    file.write("     put[[" + val[:-1] + "]"+" "*(45-len(val[:-1]))+"[mba$text.c_comma]]\n")
                else:
                    file.write("     put[[]"+" "*(45)+"[mba$text.c_comma]]\n")
            pos += 1
        elif f[pos].strip().startswith("on"):
            flag=1
            break
        else:
            pos+=1

    file.write("     put [[util.crlf]]\n\n")
    file.write("     "+"-" * 76 + "\n")
    while pos:
        if f[pos].strip().startswith("on report head"):
            file.write(f[pos])
        elif f[pos].strip().startswith("on"):
            break
        else:
            file.write(f[pos])
        pos+=1
    while pos:
        if f[pos].strip().startswith("on"):
            file.write(f[pos])
            pos+=1
        elif "column" in f[pos]:
            break
        else:
            file.write(f[pos])
            pos+=1
    flag=0
    while pos:
        words = [val.strip() for val in f[pos].split()]
        if "column" in f[pos]:
            ctx = [c.strip() for c in f[pos].split()]
            if "ctx" not in f[pos]:
                val = ""
                p = 2
                while p < len(words):
                    val += words[p]
                    p += 1
                file.write("\tput[[" + val[:-1] + "]"+" "*(100-len(val[:-1]))+"[mba$text.c_comma]]\n")
            else:
                pos += 1
                val = f[pos].strip()
                file.write("\tput[[" + val[:-1] + "]"+" "*(100-len(val[:-1]))+"[mba$text.c_comma]]\n")
        else:
            file.write(f[pos])
        pos += 1
        if "end layout" in f[pos]:
            break
    file.write("  end layout\n\n")
    file.write("  end report\n\n")

    file.close()
    print("Rep File Output Generated.")
    screen.close()

def convFileToScr(file_path):
    import re
    name = file_path.strip()
    file = open(name, 'r')
    f = file.readlines()
    y = 0
    f_input = input("ENTER OUTPUT REP SCREEN NAME: ").strip()+".txt"
    f_path = input("ENTER EXCEL FILE PATH: ").strip()+".xlsx"
    exe = pd.read_excel(f_path)
    cols = exe.columns
    data = exe[cols[0]]
    print(data)
    ptr = 0
    screen = open(f_input, 'w')
    for line in f:
        if "user_id" in line:
            words = line
            line = [val.strip() for val in line.split()]
            screen.write(words.replace(line[1][1:-1], line[1][1:-1] + "_SR"))
            continue
        if "import" not in line:
            screen.write(line)
        else:
            i = f.index(line)
            break
    while i:
        if "import" in f[i]:
            screen.write(f[i])
            i += 1
        else:
            break
    pos = len(f)
    while i:
        if "put" in f[i]:
            break
        elif "file layout" in f[i].lower():
            screen.write("  screen layout\n")
            screen.write("  order by\t\t0\n")
            screen.write("  "+"-" * 76 + "\n\n")
            i += 1
            y = 1
            print(y)
        elif "on report head" in f[i].lower():
            screen.write(f[i].replace("report", "column"))
            i += 1
            pos = i
        elif i > pos:
            i += 1
        else:
            if y == 0:
                screen.write(f[i])
            i += 1
    while i:
        if "put" not in f[i]:
            break
        else:
            line = [val.strip() for val in f[i].split()]
            p = -1
            for it in range(len(line)):
                if "[" in line[it]:
                    p = it
                    break

            st = re.search(r"\[(.*?)\]", line[p]).group(1)
            if "util.crlf" not in st:
                if ptr<len(data):
                    screen.write(f"\tcolumn {data[ptr]}" + " " * 25 + "\ttext\n")
                    screen.write("\tlabel" + " " * 25 + "\t\t")
                    screen.write((st[1:])+";")
                    ptr+=1
                else:
                    screen.write(f"\tcolumn {data[ptr]}" + " " * 25 + "\ttext\n")
                    screen.write("\tlabel" + " " * 25 + "\t\t")
                    screen.write((st[1:]) + ";")
            i += 1
            screen.write("\n\n")
    ptr=0
    while i:
        if "put" in f[i]:
            st = re.search(r"\[(.*?)\]", f[i]).group(1)
            st = st.strip()
            if "util.crlf" not in st:
                if ptr<len(data):
                    screen.write(f"\tcolumn {data[ptr]}\t\t")
                    screen.write(st[1:] + ";")
                    ptr += 1
                else:
                    screen.write(f"\tcolumn NULL\t\t")
                    screen.write(st[1:] + ";")

            screen.write("\n")
            i += 1

        else:
            screen.write(f[i])
            i += 1
            if i == len(f):
                break
    print("REP SCREEN CREATED!")
    screen.close()
    file.close()


# Function to handle the submit button click event
def submit():
    selected_option = dropdown.get()
    file_path = file_text.get()
    f = 0

    if selected_option == "Generating Text Files":
        # Run program for Use Case 1
        f = 1
    elif selected_option == "Convert Rep File to Rep Screen":
        # Run program for Use Case 2
        convFileToScr(file_path)
        f = 1

    elif selected_option == "Convert Rep Screen to Rep File":
        # Run program for Use Case 3
        convScrToFile(file_path)
        f = 1
    if f:
        text_label = tk.Label(window, text=f"{selected_option} completed!", font=font)
        text_label.pack(pady=20)
    else:
        text_label = tk.Label(window, text="Please select Use Case", font=font)
        text_label.pack(pady=20)


# Create the main window
window = tk.Tk()
window.title("Avaloq Dev Accelerator")

# Set the font size
font_size = 16
font = ("Arial", font_size)

# Dropdown for use cases
use_cases = ["Select Use Case", "Convert Rep File to Rep Screen",
             "Convert Rep Screen to Rep File"]
dropdown = ttk.Combobox(window, values=use_cases, font=font)
dropdown.set(use_cases[0])
dropdown.pack(pady=10)

# Textbox for file path
file_text = tk.Entry(window, font=font)
file_text.pack(pady=10)


# Browse button to select a file
def browse_file():
    file_path = filedialog.askopenfilename()
    file_text.delete(0, tk.END)
    file_text.insert(0, file_path)


browse_button = tk.Button(window, text="Browse", command=browse_file, font=font)
browse_button.pack(pady=10)

# Submit button to run selected program
submit_button = tk.Button(window, text="Submit", command=submit, font=font)
submit_button.pack(pady=20)

# Start the GUI main loop
window.mainloop()
