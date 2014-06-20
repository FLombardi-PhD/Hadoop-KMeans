#
# Homework 5:   exercise 3
# Author:       Federico Lombardi
# Notes:        
# instructions: just start the script

norm_file = open('GutenbergBookNorm.csv','w', encoding="utf8")
with open('GutenbergBook.csv','r', encoding="utf8") as books_dataset:
    count = 1
    lines = books_dataset.readlines()
    for line in lines:
        #book_line = books_dataset.readline()
        book_name_array_word = line.split("\t")
        book_name = book_name_array_word[0]
        print("processing book "+str(count)+" "+book_name)
        array_word = book_name_array_word[1].split(',')
        sum = 0.0
        
        for word in array_word:
            tfidf = float(word.split(':')[1])
            tfidf *= tfidf
            sum += tfidf
        print("book "+book_name+", normalizing over the sum="+str(sum)) 
        norm_sum = 0.0
        new_line = book_name+"\t"
        first = True
        
        for word_tfidf in array_word:
            word = word_tfidf.split(':')[0]
            tfidf = float(word_tfidf.split(':')[1])
            tfidf *= tfidf
            norm_tfidf = tfidf/sum
            if(first):
                new_line += word+":"+str(norm_tfidf)
                first = False
            else:
                new_line += ","+word+":"+str(norm_tfidf)
            norm_sum += norm_tfidf
        print("book "+book_name+", normalized sum: "+str(norm_sum)+"\n")
        norm_file.write(new_line+"\n")
        count += 1
        
    print("done. Processed "+str(count)+" books.")   