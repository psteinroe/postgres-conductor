export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[];

export type Database = {
  public: {
    address_book: {
      id: string;
      name: string;
      description: string | null;
      created_at: string;
      updated_at: string;
    };
    contact: {
      id: string;
      address_book_id: string;
      first_name: string;
      last_name: string | null;
      email: string | null;
      phone: string | null;
      is_favorite: boolean;
      metadata: Json | null;
      created_at: string;
    };
  };
};

