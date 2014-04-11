package se.fidde.codemintcompetition;

public class ISDRow {
    private String year;
    private int temp;

    public ISDRow(String year, int temp) {
        super();
        this.year = year;
        this.temp = temp;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

}
