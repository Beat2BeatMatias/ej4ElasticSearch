public class Item {

    private String id;
    private String siteId;
    private String title;
    private String categoryId;
    private int price;
    private String currencyId;

    public Item(){

    }
    public Item(String siteId, String title, String categoryId, int price, String currencyId) {
        this.siteId = siteId;
        this.title = title;
        this.categoryId = categoryId;
        this.price = price;
        this.currencyId = currencyId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public String getCurrencyId() {
        return currencyId;
    }

    public void setCurrencyId(String currencyId) {
        this.currencyId = currencyId;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", siteId='" + siteId + '\'' +
                ", title='" + title + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", price=" + price +
                ", currencyId='" + currencyId + '\'' +
                '}';
    }
}
