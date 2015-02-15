package beyondthewall.store.app;

public class User {
	
	/*
	Stin	
	"_id": "54dde4667244222a1b7b2cad",
    "index": 32,
    "guid": "bfbd3f46-09e8-4657-921a-6383b34eec5a",
    "isActive": false,
    "balance": "$2,090.19",
    "picture": "http://placehold.it/32x32",
    "age": 38,
    "eyeColor": "blue",
    "name": "Valerie Vaughn",
    "gender": "female",
    "company": "GOLISTIC",
    "email": "valerievaughn@golistic.com",
    "phone": "+1 (955) 590-2049",
    "address": "423 Tampa Court, Cuylerville, New York, 6184",
    "about": "Dolor cillum cillum fugiat laborum id magna ea. Reprehenderit amet consequat fugiat laborum fugiat fugiat culpa. Ad exercitation ipsum pariatur enim enim.\r\n",
    "registered": "2014-05-08T01:03:23 -06:-30",
    "latitude": -40.803369,
    "longitude": 39.213256,
    "tags": [
      "quis",
      "Lorem",
      "officia",
      "sit",
      "nisi",
      "laboris",
      "do"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Weber Oneill"
      },
      {
        "id": 1,
        "name": "Kelli Wooten"
      },
      {
        "id": 2,
        "name": "Christi Cunningham"
      }
    ],
    "greeting": "Hello, Valerie Vaughn! You have 7 unread messages.",
    "favoriteFruit": "strawberry"
*/
	
	public static class Friend{
		int id;
		String name;
	}
	
	String _id;
	int index;
	String guid;
	boolean isActive;
	String balance;
	String picture;
	int age;
	String eyeColor;
	String name;
	String gender;
	String company;
	String email;
	String phone;
	String address;
	String about;
	String registered;
	double latitude, longitude;
	String[] tags;
	Friend[] friends;
	String greeting;
	String favoriteFruit;
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public String getGuid() {
		return guid;
	}
	public void setGuid(String guid) {
		this.guid = guid;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	public String getBalance() {
		return balance;
	}
	public void setBalance(String balance) {
		this.balance = balance;
	}
	public String getPicture() {
		return picture;
	}
	public void setPicture(String picture) {
		this.picture = picture;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getEyeColor() {
		return eyeColor;
	}
	public void setEyeColor(String eyeColor) {
		this.eyeColor = eyeColor;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getAbout() {
		return about;
	}
	public void setAbout(String about) {
		this.about = about;
	}
	public String getRegistered() {
		return registered;
	}
	public void setRegistered(String registered) {
		this.registered = registered;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public String[] getTags() {
		return tags;
	}
	public void setTags(String[] tags) {
		this.tags = tags;
	}
	public Friend[] getFriends() {
		return friends;
	}
	public void setFriends(Friend[] friends) {
		this.friends = friends;
	}
	public String getGreeting() {
		return greeting;
	}
	public void setGreeting(String greeting) {
		this.greeting = greeting;
	}
	public String getFavoriteFruit() {
		return favoriteFruit;
	}
	public void setFavoriteFruit(String favoriteFruit) {
		this.favoriteFruit = favoriteFruit;
	}
	
	
	
}
