package project1.datagen;

import java.util.GregorianCalendar;

public class RandomDateGenerator {
    
    public static String generate() {

        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(2015, 2015);

        gc.set(GregorianCalendar.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(GregorianCalendar.DAY_OF_YEAR));

        gc.set(GregorianCalendar.DAY_OF_YEAR, dayOfYear);
        
        int month = randBetween(1,12);
        
        

        return (gc.get(GregorianCalendar.YEAR) + "-" + month + "-" + gc.get(GregorianCalendar.DAY_OF_MONTH));

    }

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
}
