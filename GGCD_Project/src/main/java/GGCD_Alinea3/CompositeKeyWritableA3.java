package GGCD_Alinea3;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Class que representara cada chave para o reducer saber ordenar pela forma que estiver indicada no metodo compareTo
public class CompositeKeyWritableA3 implements Writable, WritableComparable<CompositeKeyWritableA3> {

    private String tconst;
    private String originalTitle;
    private String rating;
    private String votes;
    private String genero;

    //Construtor vazio
    public CompositeKeyWritableA3() {}

    //Construtor por atribuicao
    public CompositeKeyWritableA3(String tconst, String originalTitle, String rating, String votes, String genero) {

        this.tconst = tconst;
        this.originalTitle = originalTitle;
        this.rating = rating;
        this.votes = votes;
        this.genero = genero;
    }

    //toString da class que vai ser o que vai ser escrito no ficheiro
    @Override
    public String toString() {

        StringBuilder string = new StringBuilder();
        string.append(genero);
        string.append("\t");
        string.append(tconst);
        string.append("\t");
        string.append(originalTitle);
        string.append("\t");
        string.append(rating);
        string.append("\t");
        string.append(votes);

        return string.toString();
    }

    //funcao que tem obrigatoriamente de ser implementada e que serve para ler os campos do input
    public void readFields(DataInput dataInput) throws IOException {
        tconst = WritableUtils.readString(dataInput);
        originalTitle = WritableUtils.readString(dataInput);
        rating = WritableUtils.readString(dataInput);
        votes = WritableUtils.readString(dataInput);
        genero = WritableUtils.readString(dataInput);
    }

    //funcao que tem obrigatoriamente de ser implementada e que serve para escrever o output
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, tconst);
        WritableUtils.writeString(dataOutput, originalTitle);
        WritableUtils.writeString(dataOutput, rating);
        WritableUtils.writeString(dataOutput, votes);
        WritableUtils.writeString(dataOutput, genero);
    }

    //funcao essencial para a ordenacao das chaves, e esta funcao que faz com que o
    //Secondary Sort seja bem feito
    public int compareTo(CompositeKeyWritableA3 objKeyPair) {

        int result = genero.compareTo(objKeyPair.getGenero());
        if ( result == 0) {
            Double rate = Double.parseDouble(rating);
            Double rate2 = Double.parseDouble(objKeyPair.getRating());

            result = rate.compareTo(rate2);
            if(result == 0){
               Integer votes = Integer.parseInt(this.getVotes());
               Integer votes2 = Integer.parseInt(objKeyPair.getVotes());

               result = votes.compareTo(votes2);
            }
            return result*-1; // valor negativo para descendente, valor positivo para ascendente e valor 0 para igual
        }
        else return result; //se o genero nao for igual nao posso multiplicar por -1 porque quero ascendente


    }


    //getter do id do filme
    public String getTconst() {
        return tconst;
    }

    //setter do id do filme
    public void setTconst(String tconst) {
        this.tconst = tconst;
    }

    //getter do titulo original
    public String getOriginalTitle() {
        return originalTitle;
    }

    //setter do titulo original
    public void setOriginalTitle(String originalTitle) {
        this.originalTitle = originalTitle;
    }

    //getter do rating
    public String getRating() {
        return rating;
    }

    //setter do rating
    public void setRating(String rating) {
        this.rating = rating;
    }

    //getter dos votos
    public String getVotes() {
        return votes;
    }

    //setter dos votos
    public void setVotes(String votes) {
        this.votes = votes;
    }

    //getter do genero
    public String getGenero() {
        return genero;
    }

    //setter do genero
    public void setGenero(String genero) {
        this.genero = genero;
    }
}
