
/**
 * Created by iLeonidze on 17.02.2017.
 */
public class Main {
    public static final int fakeRecordsAmount = 1000000;
    public static void main(String[] args){
        System.out.println("20.02.2017");
        DB db = new DB();
        if(!db.init()) return;
        System.out.println("Размер shops: "+db.countTableRaws("shops"));
        System.out.println("Размер categories: "+db.countTableRaws("categories"));
        System.out.println("Размер goods: "+db.countTableRaws("goods"));
        System.out.println("Размер links: "+db.countTableRaws("links"));

        /*db.dropAllTables();*/
        if(!db.isDBReady()) return;

        System.out.println("\n");
        Demo demo = new Demo(db).setFakeRecordsAmountRequired(fakeRecordsAmount);
        if(db.countTableRaws("shops")<fakeRecordsAmount) demo.insertFakeData("shops");
        if(db.countTableRaws("categories")<fakeRecordsAmount) demo.insertFakeData("categories");
        if(db.countTableRaws("goods")<fakeRecordsAmount) demo.insertFakeData("goods");
        if(db.countTableRaws("links")<fakeRecordsAmount) demo.insertFakeData("links");
    }
}